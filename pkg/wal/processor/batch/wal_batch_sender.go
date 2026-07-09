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
	sendDone         chan struct{}
	sendErrMu        sync.RWMutex
	sendErr          error
	ignoreSendErrors bool

	maxBatchBytes     int64
	maxBatchSize      int64
	batchSendInterval time.Duration
	batchBytesTuner   *batchBytesTuner[T]

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
		sendDone:          make(chan struct{}),
		once:              &sync.Once{},
		logger:            logger,
		sendBatchFn:       sendfn,
		wg:                &sync.WaitGroup{},
		cancelFn:          func() {},
		ignoreSendErrors:  config.IgnoreSendErrors,
	}

	if config.AutoTune.Enabled {
		var err error
		s.batchBytesTuner, err = newBatchBytesTuner(config.GetAutoTuneConfig(), sendfn, logger)
		if err != nil {
			return nil, fmt.Errorf("unable to create batch bytes auto tuner: %w", err)
		}
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

		if err := s.send(ctx); err != nil && !isBenignShutdownErr(err) {
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
	case <-s.sendDone:
		// sendErr is published before sendDone is closed, so any number of
		// concurrent callers can observe the underlying stop reason.
		sendErr := s.getSendErr()
		s.logger.Error(sendErr, "stop processing, sending has stopped")
		return fmt.Errorf("%w: %w", errSendStopped, sendErr)
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
			err := s.sendBatch(context.Background(), batch)
			s.queueBytesSema.Release(int64(batch.totalBytes))
			if err != nil {
				s.logger.Error(err, "failed to send batch")
				if s.ignoreSendErrors {
					continue
				}
				s.recordSendErr(err)
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
			case msg, ok := <-s.msgChan:
				if !ok {
					// Close() only closes the message channel once the writer
					// goroutine has exited, which can happen while this loop is
					// still running if a batch send failed. The send error has
					// already been recorded by the writer goroutine.
					return errSendStopped
				}
				if !msg.message.IsEmpty() && msgBatch.maxBatchBytesReached(s.getMaxBatchBytes(), msg.message) {
					if err := drainBatch(msgBatch); err != nil {
						return err
					}
				}

				msgBatch.add(msg)

				// if the batch has reached the max allowed size, don't wait for
				// the next tick and send. If we receive a keep alive, send
				// immediately.
				if s.maxBatchSizeReached(len(msgBatch.messages)) || msg.isKeepAlive() {
					s.logger.Debug("max batch size reached or keep alive received, draining batch", loglib.Fields{"batch_size": len(msgBatch.messages), "max_batch_size": s.maxBatchSize})
					if err := drainBatch(msgBatch); err != nil {
						return err
					}
				}
			}
		}
	}

	err := batchMsgLoop()
	if err != nil && !isBenignShutdownErr(err) {
		s.logger.Error(err, "sending stopped")
	}
	// publish the send error before signalling shutdown so any goroutines
	// waiting in SendMessage can observe it after the channel is closed.
	s.recordSendErr(err)
	close(s.sendDone)
	return err
}

// Close will stop the sending, and wait for all ongoing batches to finish. It
// is safe to call multiple times.
func (s *Sender[T]) Close() error {
	s.once.Do(func() {
		s.logger.Trace("closing batch sender")
		s.cancelFn()
		// wait for the send loop to finish
		s.wg.Wait()
		close(s.msgChan)
		s.logger.Trace("batch sender closed")

		if s.batchBytesTuner != nil {
			s.batchBytesTuner.close()
		}
	})

	sendErr := s.getSendErr()
	if isBenignShutdownErr(sendErr) {
		return nil
	}
	return sendErr
}

func (s *Sender[T]) recordSendErr(err error) {
	if err == nil {
		return
	}

	s.sendErrMu.Lock()
	defer s.sendErrMu.Unlock()
	if s.sendErr == nil || isBenignShutdownErr(s.sendErr) {
		s.sendErr = err
	}
}

// isBenignShutdownErr reports whether err is expected as part of a clean
// shutdown and should not be surfaced as a sender failure. It is the single
// classification point used for log suppression, send error recording
// priority and the Close return value.
func isBenignShutdownErr(err error) bool {
	return errors.Is(err, context.Canceled)
}

func (s *Sender[T]) getSendErr() error {
	s.sendErrMu.RLock()
	defer s.sendErrMu.RUnlock()
	return s.sendErr
}

func (s *Sender[T]) getMaxBatchBytes() int64 {
	if s.batchBytesTuner != nil {
		return s.batchBytesTuner.currentMaxBatchBytes(s.maxBatchBytes)
	}
	return s.maxBatchBytes
}

func (s *Sender[T]) maxBatchSizeReached(batchSize int) bool {
	// if we're automatically tuning batch bytes, we don't limit by max batch
	// size
	if s.batchBytesTuner != nil {
		return false
	}
	return int64(batchSize) >= s.maxBatchSize
}

func (s *Sender[T]) sendBatch(ctx context.Context, batch *Batch[T]) error {
	// if a batch bytes tuner is configured, and an optimal setting has not yet
	// been found, use the tuner to send the batch
	if s.batchBytesTuner != nil && !s.batchBytesTuner.hasConverged() {
		return s.batchBytesTuner.sendBatch(ctx, batch)
	}
	return s.sendBatchFn(ctx, batch)
}
