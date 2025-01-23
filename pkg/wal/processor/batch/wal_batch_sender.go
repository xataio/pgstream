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
	queueBytesSema synclib.WeightedSemaphore
	msgChan        chan (*WALMessage[T])
	once           *sync.Once
	sendDone       chan (error)
	sendErr        error

	maxBatchBytes     int64
	maxBatchSize      int64
	batchSendInterval time.Duration

	sendBatchFn sendBatchFn[T]
}

type sendBatchFn[T Message] func(context.Context, *Batch[T]) error

var errSendStopped = errors.New("stop processing, sending has stopped")

func NewSender[T Message](config *Config, sendfn sendBatchFn[T], logger loglib.Logger) (*Sender[T], error) {
	s := &Sender[T]{
		batchSendInterval: config.GetBatchTimeout(),
		maxBatchBytes:     config.GetMaxBatchBytes(),
		maxBatchSize:      config.GetMaxBatchSize(),
		msgChan:           make(chan *WALMessage[T]),
		sendDone:          make(chan error, 1),
		once:              &sync.Once{},
		logger:            logger,
		sendBatchFn:       sendfn,
	}

	maxQueueBytes, err := config.GetMaxQueueBytes()
	if err != nil {
		return nil, err
	}
	s.queueBytesSema = synclib.NewWeightedSemaphore(int64(maxQueueBytes))

	return s, nil
}

func (s *Sender[T]) AddToBatch(ctx context.Context, m *WALMessage[T]) error {
	enqueueMsg := func(ctx context.Context, msg *WALMessage[T]) error {
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

	err := enqueueMsg(ctx, m)
	// close the message channel only if the send thread has stopped, since we
	// shouldn't keep processing
	if err != nil && errors.Is(err, errSendStopped) {
		s.closeMsgChan()
	}

	return err
}

func (s *Sender[T]) Send(ctx context.Context) error {
	// make sure we send on a separate go routine to isolate the IO operations,
	// ensuring the goroutine is always sending, and minimise the wait time
	// between batch sending
	batchChan := make(chan *Batch[T])
	defer close(batchChan)
	sendErrChan := make(chan error, 1)
	go func() {
		defer close(sendErrChan)
		for batch := range batchChan {
			// If the send fails, the writer goroutine returns an error over the
			// error channel and shuts down.
			err := s.sendBatchFn(ctx, batch)
			s.queueBytesSema.Release(int64(batch.totalBytes))
			if err != nil {
				sendErrChan <- err
				return
			}
		}
	}()

	drainBatch := func(batch *Batch[T]) error {
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
				if msgBatch.maxBatchBytesReached(s.maxBatchBytes, msg.message) {
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

func (s *Sender[T]) Close() {
	s.closeMsgChan()
}

// closeMsgChan closes the internal msg channel. It can be called multiple
// times.
func (s *Sender[T]) closeMsgChan() {
	s.once.Do(func() {
		close(s.msgChan)
	})
}
