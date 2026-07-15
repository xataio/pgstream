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
	sendConcurrency   int

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
		sendConcurrency:   config.GetSendConcurrency(),
	}

	if config.AutoTune.Enabled {
		// The batch bytes auto-tuner relies on a per-batch serial-timing model
		// that is corrupted by concurrent sends, so it is incompatible with a
		// send-drainer pool. Disable it (rather than silently ignoring it) when
		// send concurrency is enabled.
		if s.sendConcurrency > 1 {
			logger.Warn(nil, "batch auto-tune is incompatible with send concurrency > 1: disabling auto-tune", loglib.Fields{"send_concurrency": s.sendConcurrency})
		} else {
			var err error
			s.batchBytesTuner, err = newBatchBytesTuner(config.GetAutoTuneConfig(), sendfn, logger)
			if err != nil {
				return nil, fmt.Errorf("unable to create batch bytes auto tuner: %w", err)
			}
		}
	}

	maxQueueBytes, err := config.GetMaxQueueBytes()
	if err != nil {
		return nil, err
	}
	s.queueBytesSema = synclib.NewWeightedSemaphore(int64(maxQueueBytes))

	// derive the cancellable context and register the send goroutine on the
	// wait group synchronously, before NewSender returns. Otherwise a caller
	// that Closes immediately races the background goroutine writing s.cancelFn
	// and calling s.wg.Add after Close's s.wg.Wait (an Add-after-Wait bug).
	ctx, s.cancelFn = context.WithCancel(ctx)
	s.wg.Add(1)

	// start the send process in the background
	go func() {
		defer s.wg.Done()
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

	drainerWg, sendErrChan, cleanupDrainers := s.startSendDrainers(batchChan)
	defer cleanupDrainers()

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
	// stop the drainers and wait for them to finish before signalling shutdown,
	// so Close (which waits on this goroutine via s.wg) never returns while a
	// COPY is still in flight.
	close(batchChan)
	drainerWg.Wait()

	// publish the send error before signalling shutdown so any goroutines
	// waiting in SendMessage can observe it after the channel is closed.
	s.recordSendErr(err)
	close(s.sendDone)
	return err
}

// startSendDrainers spawns the goroutine(s) that drain batchChan and call
// sendBatch. It returns the drainer wait group (send waits on it before
// returning, so Close never returns with a COPY still in flight), the channel
// over which the first send error is published, and a cleanup function to be
// deferred by the caller. The drainers are tracked on the returned local wait
// group, not on s.wg, which tracks the send goroutine as a whole.
func (s *Sender[T]) startSendDrainers(batchChan chan *Batch[T]) (*sync.WaitGroup, chan error, func()) {
	drainerWg := &sync.WaitGroup{}

	// send on a separate go routine (or pool of them) to isolate the IO
	// operations, ensuring a drainer is always sending, and minimise the wait
	// time between batch sending.
	if s.sendConcurrency <= 1 {
		// Single send goroutine: preserves the original ordered behavior.
		// sendBatch is called with context.Background() so an in-flight batch
		// always completes, and sendErrChan is buffered to 1. This is the path
		// used by the ordered replication writer.
		sendErrChan := make(chan error, 1)
		drainerWg.Add(1)
		go func() {
			defer drainerWg.Done()
			for batch := range batchChan {
				// If the send fails, the writer goroutine returns an error over
				// the error channel and shuts down.
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
		return drainerWg, sendErrChan, func() {}
	}

	// Pool of send drainers. All drainers range the same batchChan; the single
	// accumulator loop hands each completed batch to whichever drainer is free.
	// The first drainer to fail cancels the shared send-ctx so any in-flight
	// COPYs in the other drainers abort promptly (each batch is its own tx, so
	// cancel rolls that batch back cleanly and retry is whole-table).
	sendErrChan := make(chan error, s.sendConcurrency)
	sendCtx, cancelSend := context.WithCancel(context.Background())
	var firstErr sync.Once
	for i := 0; i < s.sendConcurrency; i++ {
		drainerWg.Add(1)
		go func() {
			defer drainerWg.Done()
			for batch := range batchChan {
				err := s.sendBatch(sendCtx, batch)
				s.queueBytesSema.Release(int64(batch.totalBytes))
				if err != nil {
					s.logger.Error(err, "failed to send batch")
					if s.ignoreSendErrors {
						continue
					}
					firstErr.Do(func() {
						s.recordSendErr(err)
						// abort in-flight COPYs in the other drainers
						cancelSend()
						sendErrChan <- err
					})
					return
				}
			}
		}()
	}
	return drainerWg, sendErrChan, cancelSend
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
