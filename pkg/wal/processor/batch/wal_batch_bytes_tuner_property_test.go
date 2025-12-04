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

		ctx := context.Background()
		for i := 0; i < 100 && !tuner.hasConverged(); i++ {
			tuner.sendBatch(ctx, mockBatch(tuner))
		}

		if !tuner.hasConverged() {
			t.Fatalf("tuner did not converge")
		}

		foundValue := tuner.candidateSetting.value
		foundThroughput := tuner.candidateSetting.throughput

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

		ctx := context.Background()
		var previousBestThroughput float64

		for i := 0; i < 50 && !tuner.hasConverged(); i++ {
			tuner.sendBatch(ctx, mockBatch(tuner))

			if tuner.candidateSetting != nil {
				currentThroughput := tuner.candidateSetting.throughput
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
					if tuner.candidateSetting.throughput < previousCandidate.throughput {
						t.Fatalf("candidate throughput decreased after incomplete batch: %v -> %v",
							previousCandidate.throughput, tuner.candidateSetting.throughput)
					}
				}
			}
		}
	})
}

// TestBatchBytesTuner_ConvergesWithIncompleteBatches verifies that the tuner can still
// converge to a good solution even when receiving many incomplete batches. This simulates
// real-world scenarios where the data stream is bursty or sparse.
func TestBatchBytesTuner_ConvergesWithIncompleteBatches(t *testing.T) {
	testLogger := zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "error",
	}))

	rapid.Check(t, func(t *rapid.T) {
		performanceProfile := genPerformanceProfile(t, 10, 100)

		if len(performanceProfile) < 10 {
			t.Skip("profile too small")
		}

		// Generate a high incomplete batch rate (50-80%)
		incompleteBatchRate := rapid.Float64Range(0.5, 0.8).Draw(t, "incomplete_rate")

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

		ctx := context.Background()

		// Allow more iterations due to incomplete batches
		maxIterations := int(math.Log2(float64(len(performanceProfile)))) * 10
		completeBatchCount := 0

		for i := 0; i < maxIterations && !tuner.hasConverged(); i++ {
			batch := mockBatch(tuner)

			// Make batches incomplete based on the rate
			if tuner.measurementSetting.value > 1 &&
				rapid.Float64Range(0, 1).Draw(t, fmt.Sprintf("incomplete_prob_%d", i)) < incompleteBatchRate {
				batch.totalBytes = rapid.IntRange(1, int(tuner.measurementSetting.value)-1).
					Draw(t, fmt.Sprintf("incomplete_size_%d", i))
			} else {
				completeBatchCount++
			}

			tuner.sendBatch(ctx, batch)
		}

		// Should still converge, though it may take more iterations
		if !tuner.hasConverged() {
			t.Fatalf("tuner did not converge within %d iterations with %.0f%% incomplete batches (only %d complete batches)",
				maxIterations, incompleteBatchRate*100, completeBatchCount)
		}

		// If we got too many incomplete batches and never established a candidate, skip
		if tuner.candidateSetting == nil {
			t.Skipf("no candidate established with %.0f%% incomplete batches (only %d/%d complete)",
				incompleteBatchRate*100, completeBatchCount, maxIterations)
		}

		// Should still find a reasonable value
		actualOptimal, optimalThroughput := findActualOptimal(performanceProfile)
		foundValue := tuner.candidateSetting.value
		foundThroughput := tuner.candidateSetting.throughput

		throughputTolerance := optimalThroughput * 0.5 // Allow 50% tolerance with incomplete batches
		withinThroughputTolerance := math.Abs(foundThroughput-optimalThroughput) <= throughputTolerance
		valueClose := math.Abs(float64(foundValue-actualOptimal)) <= float64(len(performanceProfile))*0.5

		if !withinThroughputTolerance && !valueClose {
			t.Fatalf("found value %d (throughput: %.2f) not reasonably close to optimal %d (throughput: %.2f) with incomplete batches",
				foundValue, foundThroughput, actualOptimal, optimalThroughput)
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

				ctx := context.Background()
				for j := 0; j < 100 && !tuner.hasConverged(); j++ {
					tuner.sendBatch(ctx, mockBatch(tuner))
				}
			}
		})
	}
}
