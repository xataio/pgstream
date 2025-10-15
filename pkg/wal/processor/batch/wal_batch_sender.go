// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	synclib "github.com/xataio/pgstream/internal/sync"
	loglib "github.com/xataio/pgstream/pkg/log"
)

type Sender[T Message] struct {
	logger loglib.Logger

	// queueBytesSema is used to limit the amount of memory used by the
	// unbuffered msg channel, optimising the channel performance for variable
	// size messages, while preventing the process from running oom
	queueBytesSema   synclib.WeightedSemaphore
	msgChan          chan (*WALMessage[T])
	once             *sync.Once
	sendDone         chan (error)
	sendErr          error
	ignoreSendErrors bool

	maxBatchBytes     int64
	maxBatchSize      int64
	batchSendInterval time.Duration

	wg       *sync.WaitGroup
	cancelFn context.CancelFunc

	sendBatchFn sendBatchFn[T]
}

type sendBatchFn[T Message] func(context.Context, *Batch[T]) error

var errSendStopped = errors.New("stop processing, sending has stopped")

func NewSender[T Message](ctx context.Context, config *Config, sendfn sendBatchFn[T], logger loglib.Logger) (*Sender[T], error) {
	s := &Sender[T]{
		batchSendInterval: config.GetBatchTimeout(),
		maxBatchBytes:     config.GetMaxBatchBytes(),
		maxBatchSize:      config.GetMaxBatchSize(),
		msgChan:           make(chan *WALMessage[T]),
		sendDone:          make(chan error, 1),
		once:              &sync.Once{},
		logger:            logger,
		sendBatchFn:       sendfn,
		wg:                &sync.WaitGroup{},
		cancelFn:          func() {},
		ignoreSendErrors:  config.IgnoreSendErrors,
	}

	maxQueueBytes, err := config.GetMaxQueueBytes()
	if err != nil {
		return nil, err
	}
	s.queueBytesSema = synclib.NewWeightedSemaphore(int64(maxQueueBytes))

	// start the send process in the background
	go func() {
		ctx, s.cancelFn = context.WithCancel(ctx)
		defer s.cancelFn()

		if err := s.send(ctx); err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Error(err, "sending stopped")
		}
	}()

	return s, nil
}

// SendMessage adds the message to the batch, which will be sent when the interval or the
// max number of messages is reached by a background process.
func (s *Sender[T]) SendMessage(ctx context.Context, msg *WALMessage[T]) error {
	if msg == nil {
		return nil
	}
	// make sure we don't reach the queue memory limit before adding the new
	// message to the channel. This will block until messages have been read
	// from the channel and their size is released
	msgSize := int64(msg.Size())
	if !s.queueBytesSema.TryAcquire(msgSize) {
		s.logger.Warn(nil, "batch sender: max queue bytes reached, processing blocked")
		if err := s.queueBytesSema.Acquire(ctx, msgSize); err != nil {
			return err
		}
	}

	// Add the message to the channel for processing. This will block if the
	// channel is full, or if there are no readers for the channel. If there are no
	// readers, it's likely the send thread has stopped (and therefore is no
	// longer processing), and an error will be returned.
	select {
	case s.msgChan <- msg:
	case sendDoneErr, ok := <-s.sendDone:
		// check if a different call has closed the send channel already, to
		// prevent blocking when called concurrently.
		if ok && sendDoneErr != nil {
			s.sendErr = sendDoneErr
		}
		s.logger.Error(s.sendErr, "stop processing, sending has stopped")
		return fmt.Errorf("%w: %w", errSendStopped, s.sendErr)
	}

	return nil
}

func (s *Sender[T]) send(ctx context.Context) error {
	// make sure we send on a separate go routine to isolate the IO operations,
	// ensuring the goroutine is always sending, and minimise the wait time
	// between batch sending
	batchChan := make(chan *Batch[T])
	defer close(batchChan)
	sendErrChan := make(chan error, 1)
	s.wg.Add(1)
	go func() {
		defer func() {
			close(sendErrChan)
			s.wg.Done()
		}()
		for batch := range batchChan {
			// If the send fails, the writer goroutine returns an error over the
			// error channel and shuts down.
			err := s.sendBatchFn(context.Background(), batch)
			s.queueBytesSema.Release(int64(batch.totalBytes))
			if err != nil {
				s.logger.Error(err, "failed to send batch")
				if s.ignoreSendErrors {
					continue
				}
				sendErrChan <- err
				return
			}
		}
	}()

	drainBatch := func(batch *Batch[T]) error {
		if batch.isEmpty() {
			return nil
		}

		select {
		case batchChan <- batch.drain():
		case sendErr := <-sendErrChan:
			return sendErr
		}
		return nil
	}

	batchMsgLoop := func() error {
		// we will send either as soon as the batch is full, or as soon as the
		// configured send frequency hits
		ticker := time.NewTicker(s.batchSendInterval)
		defer ticker.Stop()
		msgBatch := &Batch[T]{}
		for {
			select {
			case <-ctx.Done():
				s.logger.Debug("context terminated, draining in flight batch")
				if err := drainBatch(msgBatch); err != nil {
					return err
				}
				return ctx.Err()
			case sendErr := <-sendErrChan:
				// if there's an error while sending the batch, return the error and
				// stop sending batches
				return sendErr
			case <-ticker.C:
				if !msgBatch.isEmpty() {
					if err := drainBatch(msgBatch); err != nil {
						return err
					}
				}
			case msg := <-s.msgChan:
				if !msg.message.IsEmpty() && msgBatch.maxBatchBytesReached(s.maxBatchBytes, msg.message) {
					if err := drainBatch(msgBatch); err != nil {
						return err
					}
				}

				msgBatch.add(msg)

				// if the batch has reached the max allowed size, don't wait for
				// the next tick and send. If we receive a keep alive, send
				// immediately.
				if len(msgBatch.messages) >= int(s.maxBatchSize) || msg.isKeepAlive() {
					if err := drainBatch(msgBatch); err != nil {
						return err
					}
				}
			}
		}
	}

	err := batchMsgLoop()
	if err != nil && !errors.Is(err, context.Canceled) {
		s.logger.Error(err, "sending stopped")
	}
	s.sendDone <- err
	close(s.sendDone)
	return err
}

// Close will stop the sending, and wait for all ongoing batches to finish. It
// is safe to call multiple times.
func (s *Sender[T]) Close() {
	s.once.Do(func() {
		s.logger.Trace("closing batch sender")
		s.cancelFn()
		// wait for the send loop to finish
		s.wg.Wait()
		close(s.msgChan)
		s.logger.Trace("batch sender closed")
	})
}
