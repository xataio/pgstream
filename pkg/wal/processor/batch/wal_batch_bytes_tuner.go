// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"context"
	"fmt"
	"time"

	loglib "github.com/xataio/pgstream/pkg/log"
)

// batchBytesTuner tunes the batch bytes size based on observed throughput using
// a directional search algorithm.
type batchBytesTuner[T Message] struct {
	sendFn       sendBatchFn[T]
	logger       loglib.Logger
	throughputFn func(time.Duration, int64) float64

	maxBatchBytes        int64
	minBatchBytes        int64
	convergenceThreshold int64
	converged            bool

	measurementSetting *batchBytesSetting
	candidateSetting   *batchBytesSetting
}

type direction string

const (
	directionLeft  direction = "left"
	directionRight direction = "right"
)

type batchBytesSetting struct {
	value      int64
	throughput float64
	direction  direction
}

// Typical throughput curve
// Throughput (bytes/s)
//
//	|
//	|        ___peak___
//	|      /           \
//	|    /               \
//	|  /                   \
//	|/                       \___
//	|___________________________ Batch Size
//	tiny  small  optimal  large  huge
//
// The peak might shift right or left depending on system load and other factors.

func newBatchBytesTuner[T Message](cfg AutoTuneConfig, sendFn sendBatchFn[T], logger loglib.Logger) (*batchBytesTuner[T], error) {
	if err := cfg.IsValid(); err != nil {
		return nil, err
	}
	min := cfg.GetMinBatchBytes()
	max := cfg.GetMaxBatchBytes()

	t := &batchBytesTuner[T]{
		maxBatchBytes: max,
		minBatchBytes: min,
		sendFn:        sendFn,
		measurementSetting: &batchBytesSetting{
			value: median(min, max),
		},
		convergenceThreshold: int64(cfg.GetConvergenceThreshold() * float64(max)),
		logger: logger.WithFields(loglib.Fields{
			loglib.ModuleField: "batch_bytes_tuner",
		}),
		throughputFn: calculateThroughput,
	}

	t.logger.Info("batch bytes initialised", loglib.Fields{
		"initial_batch_bytes":   t.measurementSetting.value,
		"min_batch_bytes":       t.minBatchBytes,
		"max_batch_bytes":       t.maxBatchBytes,
		"convergence_threshold": t.convergenceThreshold,
	})

	return t, nil
}

func (t *batchBytesTuner[T]) sendBatch(ctx context.Context, batch *Batch[T]) error {
	t.logger.Trace("sending batch")

	if t.hasConverged() {
		// If the tuner has converged, no need to continue sampling.
		return t.sendFn(ctx, batch)
	}

	start := time.Now()
	defer func() {
		t.measurementSetting.throughput = t.throughputFn(time.Since(start), t.measurementSetting.value)

		nextMeasurementSetting := t.calculateNextMeasurementSetting()

		// Converge only when the range is within the threshold
		if (t.maxBatchBytes - t.minBatchBytes) <= t.convergenceThreshold {
			t.logger.Info("batch bytes tuner has converged", loglib.Fields{"final_batch_bytes": t.candidateSetting.value})
			t.converged = true
		}

		t.measurementSetting = nextMeasurementSetting
	}()
	return t.sendFn(ctx, batch)
}

func (t *batchBytesTuner[T]) calculateNextMeasurementSetting() *batchBytesSetting {
	var newMeasurementSetting *batchBytesSetting
	switch {
	case t.candidateSetting == nil:
		// if it's the first measurement, set candidate to measurement
		t.updateCandidate(t.measurementSetting)

		newMeasurementSetting = &batchBytesSetting{
			value: median(t.minBatchBytes, t.candidateSetting.value),
			// direction doesn't matter for the first measurement, just pick
			// left at random
			direction: directionLeft,
		}

	case t.measurementSetting.throughput >= t.candidateSetting.throughput:
		// if measurement has better throughput than candidate, keep going in same direction
		switch t.measurementSetting.direction {
		case directionLeft:
			// keep going left
			newMeasurementSetting = &batchBytesSetting{
				value:     median(t.minBatchBytes, t.measurementSetting.value),
				direction: directionLeft,
			}
			// We keep going left, so update the max to current
			// measurement to narrow search space on right side, since we
			// know it's worse
			t.setMaxBatchBytes(t.measurementSetting.value)
		case directionRight:
			// keep going right
			newMeasurementSetting = &batchBytesSetting{
				value:     median(t.measurementSetting.value, t.maxBatchBytes),
				direction: directionRight,
			}
			// We keep going right, so update the min to current
			// measurement to narrow search space on left side, since we
			// know it's worse
			t.setMinBatchBytes(t.measurementSetting.value)
		}

		t.updateCandidate(t.measurementSetting)

	default:
		// if measurement has lower throughput than candidate, go opposite direction
		switch t.measurementSetting.direction {
		case directionLeft:
			// if going left is not better, go right
			newMeasurementSetting = &batchBytesSetting{
				value:     median(t.measurementSetting.value, t.maxBatchBytes),
				direction: directionRight,
			}
			// We are going right now, so update the min to current
			// measurement to narrow search space on left side, since we
			// know it's worse
			t.setMinBatchBytes(t.measurementSetting.value)
		case directionRight:
			// if going right is not better, go left
			newMeasurementSetting = &batchBytesSetting{
				value:     median(t.minBatchBytes, t.measurementSetting.value),
				direction: directionLeft,
			}
			// We are going left now, so update the max to current
			// measurement to narrow search space on right side, since we
			// know it's worse
			t.setMaxBatchBytes(t.measurementSetting.value)
		}
	}

	t.logger.Trace("next measurement calculated", loglib.Fields{
		"candidate":           t.candidateSetting.String(),
		"current_measurement": t.measurementSetting.String(),
		"next_measurement":    newMeasurementSetting.String(),
		"min_batch_bytes":     t.minBatchBytes,
		"max_batch_bytes":     t.maxBatchBytes,
	})
	return newMeasurementSetting
}

func (t *batchBytesTuner[T]) hasConverged() bool {
	return t.converged
}

func (t *batchBytesTuner[T]) setMaxBatchBytes(max int64) {
	t.maxBatchBytes = max
}

func (t *batchBytesTuner[T]) setMinBatchBytes(min int64) {
	t.minBatchBytes = min
}

func (t *batchBytesTuner[T]) updateCandidate(candidate *batchBytesSetting) {
	t.candidateSetting = candidate
}

func (s *batchBytesSetting) String() string {
	if s == nil {
		return "<nil>"
	}
	return fmt.Sprintf("[value: %d, throughput: %.2fb/s, direction: %s]", s.value, s.throughput, s.direction)
}

func calculateThroughput(duration time.Duration, batchBytes int64) float64 {
	if duration == 0 {
		return 0
	}

	// bytes per second
	return float64(batchBytes) / duration.Seconds()
}

func median(min, max int64) int64 {
	return min + (max-min)/2
}
