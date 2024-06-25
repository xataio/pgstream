// SPDX-License-Identifier: Apache-2.0

package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"sync"

	httplib "github.com/xataio/pgstream/internal/http"
	synclib "github.com/xataio/pgstream/internal/sync"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook"
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
	queueBytesSema synclib.WeightedSemaphore
	notifyChan     chan *notifyMsg
	workerCount    uint
}

type subscriptionRetriever interface {
	GetSubscriptions(ctx context.Context, action, schema, table string) ([]*webhook.Subscription, error)
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
		serialiser:        json.Marshal,
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
			loglib.ServiceField: "webhook_notifier",
		})
	}
}

func WithCheckpoint(c checkpointer.Checkpoint) Option {
	return func(n *Notifier) {
		n.checkpointer = c
	}
}

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

	subscriptions := []*webhook.Subscription{}
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

	n.notifyChan <- msg

	return nil
}

func (n *Notifier) Notify(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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

func (n *Notifier) Name() string {
	return "webhooks-notifier"
}

func (n *Notifier) Close() error {
	close(n.notifyChan)
	return nil
}

func (n *Notifier) notify(ctx context.Context, msg *notifyMsg) error {
	n.logger.Trace("notifying", loglib.Fields{"urls": msg.urls})
	if len(msg.urls) > 0 {
		urlChan := make(chan string, n.workerCount)
		wg := &sync.WaitGroup{}
		for i := 0; i < int(n.workerCount); i++ {
			wg.Add(1)
			go n.webhookWorker(ctx, wg, msg.payload, urlChan)
		}

		for _, url := range msg.urls {
			urlChan <- url
		}

		close(urlChan)
		wg.Wait()
	}

	if n.checkpointer != nil {
		if err := n.checkpointer(ctx, []wal.CommitPosition{msg.commitPosition}); err != nil {
			return fmt.Errorf("checkpointing commit position: %w", err)
		}
	}

	return nil
}

func (n *Notifier) webhookWorker(ctx context.Context, wg *sync.WaitGroup, payload []byte, urls <-chan string) {
	defer wg.Done()
	for url := range urls {
		if err := n.sendWebhook(ctx, payload, url); err != nil {
			n.logger.Error(err, "sending webhook payload", loglib.Fields{
				"payload": payload,
				"url":     url,
			})
			continue
		}
	}
}

func (n *Notifier) sendWebhook(ctx context.Context, payload []byte, url string) error {
	n.logger.Trace("sending webhook", loglib.Fields{"url": url})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("building webhook payload request: %w", err)
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("sending webhook payload request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error response from payload request, status code: %s, body: %v", resp.Status, getResponseBody(resp.Body))
	}

	return nil
}

func getResponseBody(respBody io.ReadCloser) string {
	bodyBytes, err := io.ReadAll(respBody)
	if err != nil {
		return ""
	}
	return string(bodyBytes)
}
