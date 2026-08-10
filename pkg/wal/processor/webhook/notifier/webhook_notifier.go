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

// Notifier represents the process that notifies any subscribed webhooks when
// the relevant events are triggered.
type Notifier struct {
	client            httplib.Client
	logger            loglib.Logger
	checkpointer      checkpointer.Checkpoint
	subscriptionStore subscriptionRetriever
	serialiser        serialiser
	// queueBytesSema is used to limit the amount of memory used by the
	// unbuffered msg channel, optimising the channel performance for variable
	// size messages, while preventing the process from running oom
	queueBytesSema  synclib.WeightedSemaphore
	notifyChan      chan *notifyMsg
	workerCount     uint
	backoffProvider backoff.Provider
	// shutdownCh is closed by Close() to signal Notify and any in-flight
	// ProcessWALEvent calls to stop. notifyChan is never closed, so concurrent
	// senders cannot panic on "send on closed channel".
	shutdownCh chan struct{}
	notifyDone chan struct{}
	notifyErr  error
	once       *sync.Once
}

type subscriptionRetriever interface {
	GetSubscriptions(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error)
}

type Option func(*Notifier)

var errNotifyStopped = errors.New("stop processing, notify has stopped")

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
		serialiser:        json.Marshal,
		shutdownCh:        make(chan struct{}),
		notifyDone:        make(chan struct{}),
		once:              &sync.Once{},
	}

	// this allows us to bound and configure the memory used by the internal msg
	// queue
	n.queueBytesSema = synclib.NewWeightedSemaphore(cfg.maxQueueBytes())

	for _, opt := range opts {
		opt(n)
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

// ProcessWALEvent will process the wal event on input and notify all configured
// webhooks. It can be called concurrently.
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

	// make sure we don't reach the queue memory limit before adding the new
	// message to the channel. This will block until messages have been read
	// from the channel and their size is released
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
		// Close() was called before Notify processed this event. notifyChan is
		// never closed, so we cannot send into it — bail out cleanly.
		n.logger.Error(nil, "stop processing, notify is shutting down")
		return errNotifyStopped
	case <-n.notifyDone:
		// Notify has exited on its own (external ctx cancel or notify error).
		// n.notifyErr is set by Notify before closing n.notifyDone, so it is
		// safe to read here from any number of concurrent callers.
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
				// graceful shutdown via Close(); not an error
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
	// publish the notify error before signalling shutdown so any goroutines
	// waiting in ProcessWALEvent can observe it after the channel is closed.
	n.notifyErr = err
	close(n.notifyDone)
	return err
}

func (n *Notifier) Name() string {
	return "webhooks-notifier"
}

// Close signals Notify and any in-flight ProcessWALEvent callers to stop. It
// is safe to call multiple times. notifyChan itself is not closed: that would
// race with a concurrent ProcessWALEvent's send and panic.
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

		errs := make([]error, 0, len(errChan))
		for err := range errChan {
			errs = append(errs, err)
		}
		// A delivery that is still failing after exhausting retries must not
		// be checkpointed, otherwise the event is lost for good. Returning
		// the error here stops the notify loop, so the commit position is
		// only checkpointed once every webhook has been (eventually)
		// delivered, giving at-least-once delivery semantics on restart.
		if len(errs) > 0 {
			return fmt.Errorf("sending webhook notifications: %w", errors.Join(errs...))
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
			n.logger.Error(err, "sending webhook payload", loglib.Fields{
				"payload": payload,
				"url":     url,
			})
			errChan <- err
		}
	}
}

// sendWebhook posts the payload to url, retrying transient failures
// (network errors, 429s, 5xx) with the configured backoff. 4xx responses are
// treated as permanent failures and are not retried.
func (n *Notifier) sendWebhook(ctx context.Context, payload []byte, lsn, url string) error {
	n.logger.Trace("sending webhook", loglib.Fields{"url": url})

	retries := 0
	bo := n.backoffProvider(ctx)
	err := bo.RetryNotify(
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
	if err != nil {
		return fmt.Errorf("sending webhook payload request: %w", err)
	}

	return nil
}

func (n *Notifier) doSendWebhook(ctx context.Context, payload []byte, lsn, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("%w: %w", fmt.Errorf("building webhook payload request: %w", err), backoff.ErrPermanent)
	}
	// LSN is globally monotonic and unique per change, so it doubles as an
	// idempotency key, letting receivers dedupe retried/redelivered events
	// without parsing the body.
	req.Header.Set("X-Pgstream-LSN", lsn)
	req.Header.Set("Idempotency-Key", lsn)

	resp, err := n.client.Do(req)
	if err != nil {
		// network errors are transient, retry them
		return fmt.Errorf("sending webhook payload request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	respErr := fmt.Errorf("error response from payload request, status code: %s, body: %v", resp.Status, getResponseBody(resp.Body))
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError {
		// throttling and server errors are transient, retry them
		return respErr
	}
	// any other 4xx is a permanent failure, do not retry
	return fmt.Errorf("%w: %w", respErr, backoff.ErrPermanent)
}

func getResponseBody(respBody io.ReadCloser) string {
	bodyBytes, err := io.ReadAll(respBody)
	if err != nil {
		return ""
	}
	return string(bodyBytes)
}
