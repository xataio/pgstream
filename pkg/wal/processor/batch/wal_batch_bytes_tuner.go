// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"context"
	"errors"
	"fmt"
	"time"

	mathlib "github.com/xataio/pgstream/internal/math"
	loglib "github.com/xataio/pgstream/pkg/log"
)

// batchBytesTuner tunes the batch bytes size based on observed throughput using
// a directional search algorithm.
type batchBytesTuner[T Message] struct {
	sendFn sendBatchFn[T]
	logger loglib.Logger
	// calculateThroughputFn calculates throughput given duration and batch size, so it
	// can be mocked for testing to generate desired throughput curves
	calculateThroughputFn func(time.Duration, int64) float64

	maxBatchBytes             int64
	minBatchBytes             int64
	convergenceThreshold      int64
	batchBytesToleranceFactor float64
	minSamples                int
	maxSamples                int
	maxCoeficientOfVariation  float64

	measurementSetting *batchBytesSetting
	candidateSetting   *batchBytesSetting

	debugMeasurements []string
	tuningErr         error
}

const (
	minThroughputSamples             = 3
	maxThroughputSamples             = 50
	maxSkippedMeasurements           = 3
	defaultBatchBytesToleranceFactor = 0.1 // 10% tolerance when matching batch size for measurement
	maxCoeficientOfVariation         = 0.4 // max 40% CoV to consider measurements stable
)

var errNetworkTooUnstable = errors.New("network too unstable for batch bytes tuning")

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
			value: mathlib.Median(min, max),
		},
		convergenceThreshold: int64(cfg.GetConvergenceThreshold() * float64(max)),
		logger: logger.WithFields(loglib.Fields{
			loglib.ModuleField: "batch_bytes_tuner",
		}),
		calculateThroughputFn:     calculateThroughput,
		batchBytesToleranceFactor: defaultBatchBytesToleranceFactor,
		minSamples:                minThroughputSamples,
		maxSamples:                maxThroughputSamples,
		maxCoeficientOfVariation:  maxCoeficientOfVariation,
	}

	t.logger.Debug("batch bytes initialised", loglib.Fields{
		"initial_batch_bytes":   t.measurementSetting.value,
		"min_batch_bytes":       t.minBatchBytes,
		"max_batch_bytes":       t.maxBatchBytes,
		"convergence_threshold": t.convergenceThreshold,
	})

	return t, nil
}

func (t *batchBytesTuner[T]) sendBatch(ctx context.Context, batch *Batch[T]) error {
	// If it has converged, no need to continue tuning.
	if t.hasConverged() || t.hasError() {
		return t.sendFn(ctx, batch)
	}

	t.logger.Trace("sending batch", loglib.Fields{
		"batch_bytes":       batch.totalBytes,
		"measurement_bytes": t.measurementSetting.value,
	})

	// Check if the current batch size matches the measurement setting within
	// tolerance, otherwise we can't measure throughput accurately.
	if !t.measurementSetting.IsWithinTolerance(int64(batch.totalBytes), t.batchBytesToleranceFactor) {
		logFields := loglib.Fields{
			"expected_batch_bytes": t.measurementSetting.value,
			"actual_batch_bytes":   batch.totalBytes,
			"skipped_count":        t.measurementSetting.skippedCount,
		}
		switch {
		case t.measurementSetting.skippedCount >= maxSkippedMeasurements:
			// When the current batch size doesn't match the measurement setting
			// for a number of times in a row, skip this measurement and go left
			// (since we need to reduce the batch size to stop hitting the timeout).
			t.logger.Debug("skipping to next measurement due to repeated batch size mismatch", logFields)
			t.measurementSetting = t.calculateNextSetting()
			return t.sendFn(ctx, batch)
		default:
			// If the current batch size doesn't match the measurement setting,
			// skip tuning (this can happen if the sender's timeout has been triggered,
			// when there's not enough data to fill the batch size).
			t.measurementSetting.skippedCount++
			t.logger.Debug("skipping measurement due to batch size mismatch", logFields)
			return t.sendFn(ctx, batch)
		}
	}

	start := time.Now()
	defer func() {
		t.recordMeasurementAndCalculateNext(start)
	}()
	return t.sendFn(ctx, batch)
}

// recordMeasurementAndCalculateNext records the throughput measurement and
// calculates the next measurement setting once we have enough samples for a
// measurement, and they are stable enough to be representative.
func (t *batchBytesTuner[T]) recordMeasurementAndCalculateNext(start time.Time) {
	throughput := t.calculateThroughputFn(time.Since(start), t.measurementSetting.value)
	t.measurementSetting.addThroughput(throughput)

	// not enough samples yet, continue with the same measurement
	if !t.measurementSetting.hasMinSamples(t.minSamples) {
		return
	}

	t.measurementSetting.calculateAverageThroughput()

	// if measurements are not stable yet, continue collecting samples until the
	// max samples are reached.
	if !t.measurementSetting.isStable(t.maxCoeficientOfVariation) {
		logFields := loglib.Fields{
			"avg_throughput":          t.measurementSetting.avgThroughput,
			"coeficient_of_variation": t.measurementSetting.coeficientOfVariation,
			"sample_count":            len(t.measurementSetting.throughputs),
		}
		if t.measurementSetting.hasMaxSamples(t.maxSamples) {
			t.logger.Warn(errNetworkTooUnstable, "unable to tune batch bytes automatically, apply manual configuration if needed", logFields)
			t.tuningErr = errNetworkTooUnstable
			return
		}

		t.logger.Debug("measurement not stable enough, collecting more samples", logFields)
		return
	}

	t.debugMeasurements = append(t.debugMeasurements, t.measurementSetting.String())
	t.measurementSetting = t.calculateNextSetting()

	if t.hasConverged() {
		t.logger.Debug("batch bytes tuner has converged", loglib.Fields{"final_batch_bytes": t.candidateSetting.value})
	}
}

func (t *batchBytesTuner[T]) calculateNextSetting() *batchBytesSetting {
	var newMeasurementSetting *batchBytesSetting
	switch {
	case !t.measurementSetting.hasMinSamples(t.minSamples):
		// if measurement doesn't have enough samples, it means the batch send
		// failed or was skipped, so go left to reduce batch size
		newMeasurementSetting = newBatchBytesSetting(
			mathlib.Median(t.minBatchBytes, t.measurementSetting.value),
			directionLeft)
		// We are going left now, so update the max to current
		// measurement to narrow search space on right side, since we
		// know it can't be sampled successfully
		t.setMaxBatchBytes(t.measurementSetting.value)

	case t.candidateSetting == nil:
		// if it's the first measurement, set candidate to measurement
		t.updateCandidate(t.measurementSetting)

		newMeasurementSetting = newBatchBytesSetting(
			mathlib.Median(t.minBatchBytes, t.candidateSetting.value),
			// direction doesn't matter for the first measurement, just pick
			// left at random
			directionLeft,
		)

	case t.measurementSetting.avgThroughput >= t.candidateSetting.avgThroughput:
		// if measurement has better throughput than candidate, keep going in same direction
		switch t.measurementSetting.direction {
		case directionLeft:
			// keep going left
			newMeasurementSetting = newBatchBytesSetting(
				mathlib.Median(t.minBatchBytes, t.measurementSetting.value),
				directionLeft,
			)
			// We keep going left, so update the max to current
			// measurement to narrow search space on right side, since we
			// know it's worse
			t.setMaxBatchBytes(t.measurementSetting.value)
		case directionRight:
			// keep going right
			newMeasurementSetting = newBatchBytesSetting(
				mathlib.Median(t.measurementSetting.value, t.maxBatchBytes),
				directionRight,
			)
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
			newMeasurementSetting = newBatchBytesSetting(
				mathlib.Median(t.measurementSetting.value, t.maxBatchBytes),
				directionRight,
			)
			// We are going right now, so update the min to current
			// measurement to narrow search space on left side, since we
			// know it's worse
			t.setMinBatchBytes(t.measurementSetting.value)
		case directionRight:
			// if going right is not better, go left
			newMeasurementSetting = newBatchBytesSetting(
				mathlib.Median(t.minBatchBytes, t.measurementSetting.value),
				directionLeft,
			)
			// We are going left now, so update the max to current
			// measurement to narrow search space on right side, since we
			// know it's worse
			t.setMaxBatchBytes(t.measurementSetting.value)
		}
	}

	t.logger.Debug("next measurement calculated", loglib.Fields{
		"candidate":           t.candidateSetting.String(),
		"current_measurement": t.measurementSetting.String(),
		"next_measurement":    newMeasurementSetting.String(),
		"min_batch_bytes":     t.minBatchBytes,
		"max_batch_bytes":     t.maxBatchBytes,
	})
	return newMeasurementSetting
}

func (t *batchBytesTuner[T]) close() {
	t.logDebugMeasurements()
}

func (t *batchBytesTuner[T]) hasConverged() bool {
	return (t.maxBatchBytes - t.minBatchBytes) <= t.convergenceThreshold
}

func (t *batchBytesTuner[T]) hasError() bool {
	return t.tuningErr != nil
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

func (t *batchBytesTuner[T]) logDebugMeasurements() {
	result := "batch bytes measurements:\n"
	for _, measurement := range t.debugMeasurements {
		result += measurement + "\n"
	}

	if t.hasConverged() {
		result += fmt.Sprintf("final candidate:\n%s\n", t.candidateSetting.String())
	}

	t.logger.Debug(result)
}

func calculateThroughput(duration time.Duration, batchBytes int64) float64 {
	if duration == 0 {
		return 0
	}

	// bytes per second
	return float64(batchBytes) / duration.Seconds()
}
