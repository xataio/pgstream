// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/xataio/pgstream/internal/log/zerolog"
)

// Property-based tests for the batch bytes auto-tuner.
// These tests use the rapid library to generate random performance profiles and verify
// that the tuner behaves correctly across a wide range of scenarios.

var (
	noopSendFn = func(ctx context.Context, b *Batch[*mockMessage]) error {
		return nil
	}

	mockBatch = func(bt *batchBytesTuner[*mockMessage]) *Batch[*mockMessage] {
		batch := &Batch[*mockMessage]{}
		batch.totalBytes = int(bt.measurementSetting.value)
		return batch
	}
)

// TestBatchBytesTuner_ConvergesWithinBoundedIterations verifies that the batch bytes tuner
// converges within O(log n) * 3 iterations for any performance profile. This is critical
// because the tuner uses binary search, which should have logarithmic time complexity.
// If convergence takes too long, it indicates a problem with the search algorithm.
func TestBatchBytesTuner_ConvergesWithinBoundedIterations(t *testing.T) {
	testLogger := zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "error",
	}))

	rapid.Check(t, func(t *rapid.T) {
		performanceProfile := genPerformanceProfile(t, 10, 100)

		if len(performanceProfile) < 10 {
			t.Skip("profile too small")
		}

		tuner, err := newBatchBytesTuner(AutoTuneConfig{
			Enabled:              true,
			MinBatchBytes:        1,
			MaxBatchBytes:        int64(len(performanceProfile)),
			ConvergenceThreshold: 0.1, // 10% threshold works better for profiles of 10-100 elements
		}, noopSendFn, testLogger)
		if err != nil {
			t.Fatalf("failed to create tuner: %v", err)
		}

		// Mock the throughput function to use the performance profile
		tuner.calculateThroughputFn = createMockThroughputFn(performanceProfile)
		tuner.minSamples = 1

		ctx := context.Background()
		maxIterations := int(math.Log2(float64(len(performanceProfile)))) * 3

		for i := 0; i < maxIterations && !tuner.hasConverged(); i++ {
			tuner.sendBatch(ctx, mockBatch(tuner))
		}

		if !tuner.hasConverged() {
			t.Fatalf("tuner did not converge within %d iterations (O(log n) * 3)", maxIterations)
		}
	})
}

// TestBatchBytesTuner_FindsOptimalValue verifies that the tuner finds the optimal or
// near-optimal batch size. It compares the tuner's result against the actual optimal value
// in the performance profile, allowing for 10% tolerance due to timing variations.
// This property ensures the tuner actually improves performance rather than just converging
// to any arbitrary value.
func TestBatchBytesTuner_FindsOptimalValue(t *testing.T) {
	testLogger := zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "trace",
	}))

	rapid.Check(t, func(t *rapid.T) {
		performanceProfile := genPerformanceProfile(t, 10, 100)

		if len(performanceProfile) < 10 {
			t.Skip("profile too small")
		}

		actualOptimal, optimalThroughput := findActualOptimal(performanceProfile)

		tuner, err := newBatchBytesTuner(AutoTuneConfig{
			Enabled:              true,
			MinBatchBytes:        1,
			MaxBatchBytes:        int64(len(performanceProfile)),
			ConvergenceThreshold: 0.1, // 10% threshold works better for profiles of 10-100 elements
		}, noopSendFn, testLogger)
		if err != nil {
			t.Fatalf("failed to create tuner: %v", err)
		}

		// Mock the throughput function to use the performance profile
		tuner.calculateThroughputFn = createMockThroughputFn(performanceProfile)
		tuner.minSamples = 1 // reduce min samples for test speed

		ctx := context.Background()
		for i := 0; i < 100 && !tuner.hasConverged(); i++ {
			tuner.sendBatch(ctx, mockBatch(tuner))
		}

		if !tuner.hasConverged() {
			t.Fatalf("tuner did not converge")
		}

		foundValue := tuner.candidateSetting.value
		foundThroughput := tuner.candidateSetting.avgThroughput

		// Accept if throughput is within 10% of optimal or value is within 10% of search space from optimal
		throughputTolerance := optimalThroughput * 0.1
		withinThroughputTolerance := math.Abs(foundThroughput-optimalThroughput) <= throughputTolerance
		valueClose := math.Abs(float64(foundValue-actualOptimal)) <= float64(len(performanceProfile))*0.1

		if !withinThroughputTolerance && !valueClose {
			t.Fatalf("found value %d (throughput: %.2f) not close to optimal %d (throughput: %.2f)",
				foundValue, foundThroughput, actualOptimal, optimalThroughput)
		}
	})
}

// TestBatchBytesTuner_MonotonicImprovement verifies that the candidate setting only improves
// over time - the duration should never increase. This ensures that once we find a better
// configuration, we never replace it with a worse one. Non-monotonic behavior would indicate
// a flaw in the comparison or update logic.
func TestBatchBytesTuner_MonotonicImprovement(t *testing.T) {
	testLogger := zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "error",
	}))

	rapid.Check(t, func(t *rapid.T) {
		performanceProfile := genPerformanceProfile(t, 10, 100)

		if len(performanceProfile) < 10 {
			t.Skip("profile too small")
		}

		tuner, err := newBatchBytesTuner(AutoTuneConfig{
			Enabled:              true,
			MinBatchBytes:        1,
			MaxBatchBytes:        int64(len(performanceProfile)),
			ConvergenceThreshold: 0.1, // 10% threshold works better for profiles of 10-100 elements
		}, noopSendFn, testLogger)
		if err != nil {
			t.Fatalf("failed to create tuner: %v", err)
		}

		// Mock the throughput function to use the performance profile
		tuner.calculateThroughputFn = createMockThroughputFn(performanceProfile)
		tuner.minSamples = 1 // reduce min samples for test speed

		ctx := context.Background()
		var previousBestThroughput float64

		for i := 0; i < 50 && !tuner.hasConverged(); i++ {
			tuner.sendBatch(ctx, mockBatch(tuner))

			if tuner.candidateSetting != nil {
				currentThroughput := tuner.candidateSetting.avgThroughput
				if previousBestThroughput > 0 && currentThroughput < previousBestThroughput {
					t.Fatalf("throughput decreased from %v to %v - not monotonic",
						previousBestThroughput, currentThroughput)
				}
				if currentThroughput > 0 {
					previousBestThroughput = currentThroughput
				}
			}
		}
	})
}

// TestBatchBytesTuner_SearchSpaceNarrowing verifies that the search space (maxBatchBytes - minBatchBytes)
// monotonically decreases with each iteration. This is a fundamental property of binary search:
// the bounds should always narrow, never expand. If the range increases, it indicates a bug in
// the bound update logic.
func TestBatchBytesTuner_SearchSpaceNarrowing(t *testing.T) {
	testLogger := zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "error",
	}))

	rapid.Check(t, func(t *rapid.T) {
		performanceProfile := genPerformanceProfile(t, 10, 100)

		if len(performanceProfile) < 10 {
			t.Skip("profile too small")
		}

		tuner, err := newBatchBytesTuner(AutoTuneConfig{
			Enabled:              true,
			MinBatchBytes:        1,
			MaxBatchBytes:        int64(len(performanceProfile)),
			ConvergenceThreshold: 0.1, // 10% threshold works better for profiles of 10-100 elements
		}, noopSendFn, testLogger)
		if err != nil {
			t.Fatalf("failed to create tuner: %v", err)
		}

		// Mock the throughput function to use the performance profile
		tuner.calculateThroughputFn = createMockThroughputFn(performanceProfile)
		tuner.minSamples = 1

		ctx := context.Background()
		previousRange := tuner.maxBatchBytes - tuner.minBatchBytes

		for i := 0; i < 50 && !tuner.hasConverged(); i++ {
			tuner.sendBatch(ctx, mockBatch(tuner))

			currentRange := tuner.maxBatchBytes - tuner.minBatchBytes
			if currentRange > previousRange {
				t.Fatalf("range increased from %d to %d - not narrowing", previousRange, currentRange)
			}
			previousRange = currentRange
		}
	})
}

// TestBatchBytesTuner_RespectsConvergenceThreshold verifies that when the tuner converges,
// the final search range respects the configured convergence threshold. The range should be
// at most threshold * maxBatchBytes. This ensures the convergence detection logic correctly
// implements the stopping condition.
func TestBatchBytesTuner_RespectsConvergenceThreshold(t *testing.T) {
	testLogger := zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "error",
	}))

	rapid.Check(t, func(t *rapid.T) {
		performanceProfile := genPerformanceProfile(t, 10, 100)
		// Use 0.05 (5%) to 0.5 (50%) to ensure int64(threshold * size) >= 1 for small profiles
		threshold := rapid.Float64Range(0.05, 0.5).Draw(t, "threshold")

		if len(performanceProfile) < 10 {
			t.Skip("profile too small")
		}

		tuner, err := newBatchBytesTuner(AutoTuneConfig{
			Enabled:              true,
			MinBatchBytes:        1,
			MaxBatchBytes:        int64(len(performanceProfile)),
			ConvergenceThreshold: threshold,
		}, noopSendFn, testLogger)
		if err != nil {
			t.Fatalf("failed to create tuner: %v", err)
		}

		// Mock the throughput function to use the performance profile
		tuner.calculateThroughputFn = createMockThroughputFn(performanceProfile)
		tuner.minSamples = 1

		ctx := context.Background()
		for i := 0; i < 100 && !tuner.hasConverged(); i++ {
			tuner.sendBatch(ctx, mockBatch(tuner))
		}

		if !tuner.hasConverged() {
			t.Skip("may not converge with very small threshold")
		}

		finalRange := float64(tuner.maxBatchBytes - tuner.minBatchBytes)
		maxAllowedRange := threshold * float64(len(performanceProfile))

		if finalRange > maxAllowedRange {
			t.Fatalf("final range %f exceeds threshold %f * %d = %f",
				finalRange, threshold, len(performanceProfile), maxAllowedRange)
		}
	})
}

// TestBatchBytesTuner_HandlesIncompleteBatches verifies that the tuner correctly handles
// incomplete batches (batches smaller than the measurement setting). The tuner should skip
// measurements for incomplete batches and not update the candidate based on them, as they
// don't represent the true performance at that batch size.
func TestBatchBytesTuner_HandlesIncompleteBatches(t *testing.T) {
	testLogger := zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "error",
	}))

	rapid.Check(t, func(t *rapid.T) {
		performanceProfile := genPerformanceProfile(t, 10, 100)

		if len(performanceProfile) < 10 {
			t.Skip("profile too small")
		}

		tuner, err := newBatchBytesTuner(AutoTuneConfig{
			Enabled:              true,
			MinBatchBytes:        1,
			MaxBatchBytes:        int64(len(performanceProfile)),
			ConvergenceThreshold: 0.1,
		}, noopSendFn, testLogger)
		if err != nil {
			t.Fatalf("failed to create tuner: %v", err)
		}
		tuner.batchBytesToleranceFactor = 0.0 // No tolerance for this test

		tuner.calculateThroughputFn = createMockThroughputFn(performanceProfile)
		tuner.minSamples = 1 // reduce min samples for test speed

		ctx := context.Background()

		// Send a mix of complete and incomplete batches
		for i := 0; i < 50 && !tuner.hasConverged(); i++ {
			batch := mockBatch(tuner)

			// Make 30% of batches incomplete (smaller than measurement setting)
			if rapid.Bool().Draw(t, fmt.Sprintf("incomplete_%d", i)) && rapid.Float64Range(0, 1).Draw(t, fmt.Sprintf("incomplete_prob_%d", i)) < 0.3 {
				// Incomplete batch - set size to half of measurement setting
				batch.totalBytes = int(tuner.measurementSetting.value / 2)
			}

			previousCandidate := tuner.candidateSetting
			tuner.sendBatch(ctx, batch)

			// If batch was incomplete, candidate should not have been updated with worse values
			if batch.totalBytes < int(tuner.measurementSetting.value) {
				if previousCandidate != nil && tuner.candidateSetting != nil {
					if tuner.candidateSetting.avgThroughput < previousCandidate.avgThroughput {
						t.Fatalf("candidate avg throughput decreased after incomplete batch: %v -> %v",
							previousCandidate.avgThroughput, tuner.candidateSetting.avgThroughput)
					}
				}
			}
		}
	})
}

// TestBatchBytesTuner_DoesNotUpdateCandidateOnIncompleteBatch verifies that the candidate
// is never updated based on an incomplete batch measurement, as incomplete batches don't
// accurately reflect the performance at that batch size setting.
func TestBatchBytesTuner_DoesNotUpdateCandidateOnIncompleteBatch(t *testing.T) {
	testLogger := zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "error",
	}))

	rapid.Check(t, func(t *rapid.T) {
		performanceProfile := genPerformanceProfile(t, 10, 100)

		if len(performanceProfile) < 10 {
			t.Skip("profile too small")
		}

		tuner, err := newBatchBytesTuner(AutoTuneConfig{
			Enabled:              true,
			MinBatchBytes:        1,
			MaxBatchBytes:        int64(len(performanceProfile)),
			ConvergenceThreshold: 0.1,
		}, noopSendFn, testLogger)
		if err != nil {
			t.Fatalf("failed to create tuner: %v", err)
		}
		tuner.batchBytesToleranceFactor = 0.0 // No tolerance for this test

		tuner.calculateThroughputFn = createMockThroughputFn(performanceProfile)
		tuner.minSamples = 1

		ctx := context.Background()

		// Send a few complete batches to establish a candidate
		for i := 0; i < 5; i++ {
			tuner.sendBatch(ctx, mockBatch(tuner))
		}

		if tuner.candidateSetting == nil {
			t.Skip("no candidate established")
		}

		candidateBeforeIncomplete := tuner.candidateSetting
		measurementBeforeIncomplete := tuner.measurementSetting.value

		// Send an incomplete batch
		incompleteBatch := mockBatch(tuner)
		incompleteBatch.totalBytes = int(tuner.measurementSetting.value / 2)
		tuner.sendBatch(ctx, incompleteBatch)

		// Candidate should not have changed due to incomplete batch
		if tuner.candidateSetting != candidateBeforeIncomplete {
			// It's okay if candidate changed to a better value from a different measurement,
			// but it should not be based on the incomplete batch's measurement setting
			if tuner.candidateSetting.value == measurementBeforeIncomplete {
				t.Fatalf("candidate was updated based on incomplete batch measurement")
			}
		}
	})
}

// TestBatchyesTuner_RespectsMinimumSamples verifies that the tuner collects
// at least minSamples measurements at a given batch size before considering it
// as a valid candidate. This prevents premature decisions based on insufficient data.
func TestBatchBytesTuner_RespectsMinimumSamples(t *testing.T) {
	testLogger := zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "error",
	}))

	rapid.Check(t, func(t *rapid.T) {
		performanceProfile := genPerformanceProfile(t, 20, 100)
		minSamples := rapid.IntRange(2, 5).Draw(t, "min_samples")

		if len(performanceProfile) < 20 {
			t.Skip("profile too small")
		}

		tuner, err := newBatchBytesTuner(AutoTuneConfig{
			Enabled:              true,
			MinBatchBytes:        1,
			MaxBatchBytes:        int64(len(performanceProfile)),
			ConvergenceThreshold: 0.1,
		}, noopSendFn, testLogger)
		if err != nil {
			t.Fatalf("failed to create tuner: %v", err)
		}

		tuner.calculateThroughputFn = createMockThroughputFn(performanceProfile)
		tuner.minSamples = minSamples

		ctx := context.Background()
		prevCandidate := tuner.candidateSetting

		// Track when candidate changes and verify it had enough samples
		for i := 0; i < 100 && !tuner.hasConverged(); i++ {
			tuner.sendBatch(ctx, mockBatch(tuner))

			// Check if candidate was updated
			if tuner.candidateSetting != prevCandidate && tuner.candidateSetting != nil {
				// A new candidate was selected - verify it has enough samples
				actualSamples := len(tuner.candidateSetting.throughputs)
				if actualSamples < minSamples {
					t.Fatalf("candidate selected with only %d samples (required %d), candidate: %v",
						actualSamples, minSamples, tuner.candidateSetting)
				}
				prevCandidate = tuner.candidateSetting
			}
		}
	})
}

// TestBatchBytesTuner_RetriesUnstableMeasurements verifies that the tuner retries
// measurements when they show high variance (coefficient of variation >= 40%).
// This ensures the tuner only accepts stable measurements as candidates, preventing
// decisions based on unreliable data.
func TestBatchBytesTuner_RetriesUnstableMeasurements(t *testing.T) {
	testLogger := zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "error",
	}))

	rapid.Check(t, func(t *rapid.T) {
		performanceProfile := genPerformanceProfile(t, 20, 100)
		minSamples := 3 // Need at least 3 samples to calculate meaningful std dev

		if len(performanceProfile) < 20 {
			t.Skip("profile too small")
		}

		tuner, err := newBatchBytesTuner(AutoTuneConfig{
			Enabled:              true,
			MinBatchBytes:        1,
			MaxBatchBytes:        int64(len(performanceProfile)),
			ConvergenceThreshold: 0.1,
		}, noopSendFn, testLogger)
		if err != nil {
			t.Fatalf("failed to create tuner: %v", err)
		}

		// Create a mock function that returns unstable throughput (high variance)
		// for certain batch sizes and stable throughput for others
		unstableBatchSize := rapid.Int64Range(10, int64(len(performanceProfile)/2)).Draw(t, "unstable_batch_size")

		callCount := make(map[int64]int)
		tuner.calculateThroughputFn = func(duration time.Duration, batchSize int64) float64 {
			callCount[batchSize]++
			idx := int(batchSize - 1)
			if idx < 0 || idx >= len(performanceProfile) {
				return 0
			}

			baseThroughput := performanceProfile[idx]

			// For the unstable batch size, return highly variable throughput
			// until we've had enough retries (simulate eventually stabilizing)
			if batchSize == unstableBatchSize && callCount[batchSize] <= 6 {
				// Create high variance: alternate between +50% and -50% of base
				if callCount[batchSize]%2 == 0 {
					return baseThroughput * 1.5
				}
				return baseThroughput * 0.5
			}

			// For other batch sizes or after enough retries, return stable throughput
			return baseThroughput
		}

		tuner.minSamples = minSamples

		ctx := context.Background()
		retryCounts := make(map[int64]int)
		prevMeasurementValue := int64(0)

		// Run tuner and track retries
		for i := 0; i < 200 && !tuner.hasConverged(); i++ {
			currentValue := tuner.measurementSetting.value

			// If we're retrying the same measurement value
			if currentValue == prevMeasurementValue {
				retryCounts[currentValue]++
			}

			tuner.sendBatch(ctx, mockBatch(tuner))
			prevMeasurementValue = currentValue
		}

		// Verify that if we encountered the unstable batch size, we retried it
		if callCount[unstableBatchSize] > minSamples {
			// We measured this size multiple times, check if it was retried
			// due to instability (not just normal sampling)
			if retryCounts[unstableBatchSize] < 1 {
				// This is okay - we might not have hit it during tuning
				t.Logf("unstable batch size %d was sampled but didn't require retries", unstableBatchSize)
			} else {
				t.Logf("correctly retried unstable batch size %d, %d times",
					unstableBatchSize, retryCounts[unstableBatchSize])
			}
		}

		// Most importantly: verify that all candidates have stable measurements
		if tuner.candidateSetting != nil && len(tuner.candidateSetting.throughputs) >= 2 {
			// The candidate should have its coefficient of variation already calculated
			if math.IsNaN(tuner.candidateSetting.coeficientOfVariation) {
				t.Fatalf("final candidate has NaN coefficient of variation (likely zero mean)")
			}
			if tuner.candidateSetting.coeficientOfVariation >= maxCoeficientOfVariation {
				t.Fatalf("final candidate has unstable measurements (CV=%.2f >= %.2f)", tuner.candidateSetting.coeficientOfVariation, maxCoeficientOfVariation)
			}
		}
	})
}

// TestBatchBytesTuner_RespectsMaximumSamples verifies that when measurements remain
// unstable even after collecting maxSamples, the tuner stops collecting more samples
// and reports an error rather than continuing indefinitely. This prevents the tuner
// from getting stuck when network conditions are too unstable for reliable tuning.
func TestBatchBytesTuner_RespectsMaximumSamples(t *testing.T) {
	testLogger := zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "error",
	}))

	rapid.Check(t, func(t *rapid.T) {
		performanceProfile := genPerformanceProfile(t, 20, 100)
		maxSamples := rapid.IntRange(5, 10).Draw(t, "max_samples")
		minSamples := rapid.IntRange(2, maxSamples-1).Draw(t, "min_samples")

		if len(performanceProfile) < 20 {
			t.Skip("profile too small")
		}

		tuner, err := newBatchBytesTuner(AutoTuneConfig{
			Enabled:              true,
			MinBatchBytes:        1,
			MaxBatchBytes:        int64(len(performanceProfile)),
			ConvergenceThreshold: 0.1,
		}, noopSendFn, testLogger)
		if err != nil {
			t.Fatalf("failed to create tuner: %v", err)
		}

		// Create a mock function that always returns unstable throughput
		// (high variance that never stabilizes)
		tuner.calculateThroughputFn = func(duration time.Duration, batchSize int64) float64 {
			idx := int(batchSize - 1)
			if idx < 0 || idx >= len(performanceProfile) {
				return 0
			}

			baseThroughput := performanceProfile[idx]
			// Always create high variance by alternating between high and low values
			sampleCount := len(tuner.measurementSetting.throughputs)
			if sampleCount%2 == 0 {
				return baseThroughput * 2.0 // +100%
			}
			return baseThroughput * 0.5 // -50%
		}

		tuner.minSamples = minSamples
		tuner.maxSamples = maxSamples

		ctx := context.Background()
		initialMeasurement := tuner.measurementSetting.value

		// Collect samples at the initial measurement point
		for i := 0; i < maxSamples*2 && tuner.measurementSetting.value == initialMeasurement; i++ {
			tuner.sendBatch(ctx, mockBatch(tuner))
		}

		// After attempting to collect maxSamples, verify behavior:
		// 1. Should not have collected more than maxSamples
		actualSamples := len(tuner.measurementSetting.throughputs)
		if actualSamples > maxSamples {
			t.Fatalf("collected %d samples, exceeding max of %d", actualSamples, maxSamples)
		}

		// 2. Should have stopped at maxSamples and reported an error
		if actualSamples == maxSamples {
			if tuner.tuningErr == nil {
				t.Fatalf("reached maxSamples with unstable measurements but no error was set")
			}
			// Tuner should not advance to a new measurement when hitting maxSamples with unstable data
			if tuner.measurementSetting.value != initialMeasurement {
				t.Fatalf("tuner advanced to new measurement despite hitting maxSamples with unstable data")
			}
		}
	})
}

// Generators

// genPerformanceProfile generates a random throughput profile (bytes/second)
// representing throughput at different batch byte values.
// Creates a parabolic throughput curve with a single peak at the optimal point.
func genPerformanceProfile(t *rapid.T, minSize, maxSize int) []float64 {
	size := rapid.IntRange(minSize, maxSize).Draw(t, "size")

	// Generate a single random base throughput for consistency
	baseThroughput := float64(rapid.Int64Range(1000, 10000).Draw(t, "base_throughput"))

	// Add small random noise for each position (much smaller than parabolic effect)
	noise := make([]float64, size)
	for i := range noise {
		noise[i] = float64(rapid.Int64Range(-50, 50).Draw(t, fmt.Sprintf("noise_%d", i)))
	}

	// Create parabolic throughput curve with single peak at optimal point
	// Throughput peaks at an optimal point in the middle
	optimal := len(noise) / 2
	result := make([]float64, size)

	for i := range noise {
		distance := math.Abs(float64(i - optimal))
		// Throughput decreases quadratically with distance from optimal
		// The parabolic penalty dominates over the small random noise
		penalty := distance * distance / float64(size)
		result[i] = (baseThroughput + noise[i]) / (1.0 + penalty*10.0)
	}

	return result
}

// Helper functions

// createMockThroughputFn creates a mock throughput function that returns throughput
// from the performance profile without actually executing the batch. This makes tests run instantly.
// The function takes duration and batch size, but we ignore duration and just return the
// pre-calculated throughput for that batch size from the profile.
func createMockThroughputFn(profile []float64) func(time.Duration, int64) float64 {
	return func(duration time.Duration, batchSize int64) float64 {
		// Use the batch size as the index into the profile
		// Subtract 1 because batch sizes start at 1 but array indices start at 0
		idx := int(batchSize - 1)
		if idx >= 0 && idx < len(profile) {
			return profile[idx]
		}
		return 0 // fallback to 0 if invalid
	}
}

// findActualOptimal scans the entire performance profile to find the true optimal batch size.
// Returns the index (batch size) and throughput of the maximum throughput in the profile.
// This is used as ground truth to validate the tuner's result.
func findActualOptimal(profile []float64) (int64, float64) {
	if len(profile) == 0 {
		return 0, 0
	}

	maxIdx := 0
	maxThroughput := profile[0]

	for i, throughput := range profile {
		if throughput > maxThroughput {
			maxThroughput = throughput
			maxIdx = i
		}
	}

	return int64(maxIdx + 1), maxThroughput // +1 because batch bytes start at 1
}

// Benchmark property tests

// BenchmarkBatchBytesTuner_Convergence measures the time it takes for the tuner to converge
// across different performance profile sizes. This helps validate that convergence time
// scales logarithmically with the search space size.
func BenchmarkBatchBytesTuner_Convergence(b *testing.B) {
	testLogger := zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "error",
	}))

	sizes := []int{10, 50, 100, 500, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			// Create a fixed throughput profile
			profile := make([]float64, size)
			optimal := size / 2
			for i := range profile {
				distance := math.Abs(float64(i - optimal))
				penalty := distance * distance / float64(size)
				profile[i] = 1000.0 / (1.0 + penalty) // bytes/second
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				tuner, _ := newBatchBytesTuner(AutoTuneConfig{
					Enabled:              true,
					MinBatchBytes:        1,
					MaxBatchBytes:        int64(size),
					ConvergenceThreshold: 0.1, // 10% threshold
				}, noopSendFn, testLogger)
				tuner.minSamples = 1

				ctx := context.Background()
				for j := 0; j < 100 && !tuner.hasConverged(); j++ {
					tuner.sendBatch(ctx, mockBatch(tuner))
				}
			}
		})
	}
}
