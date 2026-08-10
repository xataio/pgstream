// SPDX-License-Identifier: Apache-2.0

package notifier

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	httplib "github.com/xataio/pgstream/internal/http"
	"github.com/xataio/pgstream/internal/json"
	synclib "github.com/xataio/pgstream/internal/sync"
	"github.com/xataio/pgstream/pkg/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription"
)

const maxResponseBodyBytes = 4 * 1024

var errNotifyStopped = errors.New("stop processing, notify has stopped")

type Notifier struct {
	client            httplib.Client
	logger            loglib.Logger
	checkpointer      checkpointer.Checkpoint
	subscriptionStore subscriptionRetriever
	serialiser        serialiser
	// bounds memory used by inflight msgs
	queueBytesSema  synclib.WeightedSemaphore
	notifyChan      chan *notifyMsg
	workerCount     uint
	backoffProvider backoff.Provider
	strictMode      bool
	// notifyChan never closes, avoids send panic
	shutdownCh chan struct{}
	notifyDone chan struct{}
	notifyErr  error
	once       *sync.Once
}

type subscriptionRetriever interface {
	GetSubscriptions(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error)
}

type Option func(*Notifier)

func New(cfg *Config, store subscriptionRetriever, opts ...Option) *Notifier {
	n := &Notifier{
		logger: loglib.NewNoopLogger(),
		client: &http.Client{
			Timeout: cfg.clientTimeout(),
		},
		subscriptionStore: store,
		notifyChan:        make(chan *notifyMsg),
		workerCount:       cfg.workerCount(),
		backoffProvider:   backoff.NewProvider(cfg.backoffConfig()),
		strictMode:        cfg.StrictMode,
		serialiser:        json.Marshal,
		shutdownCh:        make(chan struct{}),
		notifyDone:        make(chan struct{}),
		once:              &sync.Once{},
	}

	n.queueBytesSema = synclib.NewWeightedSemaphore(cfg.maxQueueBytes())

	for _, opt := range opts {
		opt(n)
	}

	if !cfg.StrictMode {
		n.logger.Info("strict_mode is disabled: permanently failing webhook deliveries will be dropped and logged rather than stopping the pipeline")
	}

	return n
}

func WithLogger(l loglib.Logger) Option {
	return func(n *Notifier) {
		n.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "webhook_notifier",
		})
	}
}

func WithCheckpoint(c checkpointer.Checkpoint) Option {
	return func(n *Notifier) {
		n.checkpointer = c
	}
}

// safe for concurrent calls
func (n *Notifier) ProcessWALEvent(ctx context.Context, walEvent *wal.Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			n.logger.Panic("[PANIC] Panic while processing replication event", loglib.Fields{
				"wal_data":    walEvent.Data,
				"panic":       r,
				"stack_trace": debug.Stack(),
			})
			err = fmt.Errorf("webhook notifier: %w: %v", processor.ErrPanic, r)
		}
	}()

	subscriptions := []*subscription.Subscription{}
	if walEvent.Data != nil {
		data := walEvent.Data
		subscriptions, err = n.subscriptionStore.GetSubscriptions(ctx, data.Action, data.Schema, data.Table)
		if err != nil {
			return fmt.Errorf("retrieving subscriptions: %w", err)
		}
		n.logger.Debug("matching subscriptions", loglib.Fields{"subscriptions": subscriptions})
	}

	msg, err := newNotifyMsg(walEvent, subscriptions, n.serialiser)
	if err != nil {
		return err
	}

	// blocks until queue has room
	msgSize := int64(msg.size())
	if !n.queueBytesSema.TryAcquire(msgSize) {
		n.logger.Warn(nil, "webhook notifier: max queue bytes reached, processing blocked")
		if err := n.queueBytesSema.Acquire(ctx, msgSize); err != nil {
			return err
		}
	}

	select {
	case n.notifyChan <- msg:
	case <-n.shutdownCh:
		// notifyChan never closed, bail cleanly
		n.logger.Error(nil, "stop processing, notify is shutting down")
		return errNotifyStopped
	case <-n.notifyDone:
		// notifyErr set before channel closes
		n.logger.Error(n.notifyErr, "stop processing, notify has stopped")
		if n.notifyErr == nil {
			return errNotifyStopped
		}
		return fmt.Errorf("%w: %w", errNotifyStopped, n.notifyErr)
	}

	return nil
}

func (n *Notifier) Notify(ctx context.Context) error {
	notifyLoop := func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-n.shutdownCh:
				// graceful shutdown, not an error
				return nil
			case msg := <-n.notifyChan:
				err := n.notify(ctx, msg)
				n.queueBytesSema.Release(int64(msg.size()))
				if err != nil {
					n.logger.Error(err, "sending webhook event", loglib.Fields{
						"urls":            msg.urls,
						"commit position": msg.commitPosition,
						"payload":         string(msg.payload),
					})
					return fmt.Errorf("sending webhook event: %w", err)
				}
			}
		}
	}

	err := notifyLoop()
	// set error before closing channel
	n.notifyErr = err
	close(n.notifyDone)
	return err
}

func (n *Notifier) Name() string {
	return "webhooks-notifier"
}

// idempotent; notifyChan stays open
func (n *Notifier) Close() error {
	n.once.Do(func() {
		close(n.shutdownCh)
	})
	return nil
}

func (n *Notifier) notify(ctx context.Context, msg *notifyMsg) error {
	n.logger.Trace("notifying", loglib.Fields{"urls": msg.urls})
	if len(msg.urls) > 0 {
		urlChan := make(chan string, n.workerCount)
		errChan := make(chan error, len(msg.urls))
		wg := &sync.WaitGroup{}
		for i := 0; i < int(n.workerCount); i++ {
			wg.Add(1)
			go n.webhookWorker(ctx, wg, msg.payload, msg.lsn, urlChan, errChan)
		}

		for _, url := range msg.urls {
			urlChan <- url
		}

		close(urlChan)
		wg.Wait()
		close(errChan)

		// in strict mode, permanent errors block like any other
		var blockingErrs []error
		for err := range errChan {
			if !n.strictMode && errors.Is(err, backoff.ErrPermanent) {
				n.logger.Error(err, "webhook delivery permanently failed, dropping")
				continue
			}
			blockingErrs = append(blockingErrs, err)
		}
		if len(blockingErrs) > 0 {
			return fmt.Errorf("sending webhook notifications: %w", errors.Join(blockingErrs...))
		}
	}

	if n.checkpointer != nil {
		if err := n.checkpointer(ctx, []wal.CommitPosition{msg.commitPosition}); err != nil {
			return fmt.Errorf("checkpointing commit position: %w", err)
		}
	}

	return nil
}

func (n *Notifier) webhookWorker(ctx context.Context, wg *sync.WaitGroup, payload []byte, lsn string, urls <-chan string, errChan chan<- error) {
	defer wg.Done()
	for url := range urls {
		if err := n.sendWebhook(ctx, payload, lsn, url); err != nil {
			errChan <- fmt.Errorf("webhook url %s: %w", url, err)
		}
	}
}

func (n *Notifier) sendWebhook(ctx context.Context, payload []byte, lsn, url string) error {
	n.logger.Trace("sending webhook", loglib.Fields{"url": url})

	retries := 0
	bo := n.backoffProvider(ctx)
	return bo.RetryNotify(
		func() error {
			return n.doSendWebhook(ctx, payload, lsn, url)
		},
		func(err error, d time.Duration) {
			retries++
			n.logger.Warn(err, "retrying webhook delivery", loglib.Fields{
				"url":     url,
				"backoff": d,
				"retries": retries,
			})
		})
}

func (n *Notifier) doSendWebhook(ctx context.Context, payload []byte, lsn, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("%w: building webhook payload request: %w", backoff.ErrPermanent, err)
	}
	// snapshot rows share the zero LSN
	if lsn != "" && lsn != wal.ZeroLSN {
		req.Header.Set("X-Pgstream-LSN", lsn)
		req.Header.Set("Idempotency-Key", lsn)
	}

	resp, err := n.client.Do(req)
	if err != nil {
		// network errors are retryable
		return fmt.Errorf("sending webhook payload request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return nil
	}

	respErr := fmt.Errorf("error response from payload request, status code: %s, body: %v", resp.Status, getResponseBody(resp.Body))
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError {
		// 429/5xx are retryable
		return respErr
	}
	// other non-2xx: permanent, no retry
	return fmt.Errorf("%w: %w", backoff.ErrPermanent, respErr)
}

func getResponseBody(respBody io.ReadCloser) string {
	bodyBytes, err := io.ReadAll(io.LimitReader(respBody, maxResponseBodyBytes))
	if err != nil {
		return ""
	}
	return string(bodyBytes)
}
