// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/log/zerolog"
)

func TestBatchBytesTuner_sendBatch(t *testing.T) {
	t.Parallel()

	testLogger := zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "trace",
	}))

	testMockBatch := func(bt *batchBytesTuner[*mockMessage]) *Batch[*mockMessage] {
		batch := &Batch[*mockMessage]{}
		batch.totalBytes = int(bt.measurementSetting.value)
		return batch
	}

	tests := []struct {
		name                  string
		calculateThroughputFn func(time.Duration, int64) float64
		batch                 func(*batchBytesTuner[*mockMessage]) *Batch[*mockMessage]

		wantCandidate *batchBytesSetting
		wantConverged bool
		wantErr       error
	}{
		{
			name: "converged after going left",
			calculateThroughputFn: func(duration time.Duration, batchBytes int64) float64 {
				// simulate a parabolic throughput curve with peak at 20 bytes
				optimalBatchBytes := int64(20)
				if batchBytes == optimalBatchBytes {
					return 1000.0 // peak throughput
				}
				// Throughput decreases quadratically as we move away from the optimal point
				distance := float64(batchBytes - optimalBatchBytes)
				throughput := 1000.0 - (distance * distance * 0.4)
				if throughput < 0 {
					return 0
				}
				return throughput
			},
			batch: testMockBatch,

			wantCandidate: &batchBytesSetting{
				value:         20,
				throughputs:   []float64{1000.0},
				avgThroughput: 1000.0,
				direction:     directionLeft,
			},
			wantConverged: true,
			wantErr:       nil,
		},
		{
			name: "converged to initial value",
			calculateThroughputFn: func(duration time.Duration, batchBytes int64) float64 {
				// simulate a parabolic throughput curve with peak at 50 bytes
				optimalBatchBytes := int64(50)
				if batchBytes == optimalBatchBytes {
					return 1000.0 // peak throughput
				}
				// Throughput decreases quadratically as we move away from the optimal point
				distance := float64(batchBytes - optimalBatchBytes)
				throughput := 1000.0 - (distance * distance * 0.4)
				if throughput < 0 {
					return 0
				}
				return throughput
			},
			batch: testMockBatch,

			wantCandidate: &batchBytesSetting{
				value:         50,
				throughputs:   []float64{1000.0},
				avgThroughput: 1000.0,
				direction:     "",
			},
			wantConverged: true,
			wantErr:       nil,
		},
		{
			name: "converged after going right",
			calculateThroughputFn: func(duration time.Duration, batchBytes int64) float64 {
				// simulate a parabolic throughput curve with peak at 50 bytes
				optimalBatchBytes := int64(81)
				if batchBytes == optimalBatchBytes {
					return 1000.0 // peak throughput
				}
				// Throughput decreases quadratically as we move away from the optimal point
				distance := float64(batchBytes - optimalBatchBytes)
				throughput := 1000.0 - (distance * distance * 0.4)
				if throughput < 0 {
					return 0.01
				}
				return throughput
			},
			batch: testMockBatch,

			wantCandidate: &batchBytesSetting{
				value:         81,
				throughputs:   []float64{1000.0},
				avgThroughput: 1000.0,
				direction:     directionRight,
			},
			wantConverged: true,
			wantErr:       nil,
		},
		{
			name: "skipped measurements - no convergence",
			calculateThroughputFn: func(duration time.Duration, batchBytes int64) float64 {
				t.Fatal("calculateThroughputFn should not be called")
				return -1.0
			},
			batch: func(bbt *batchBytesTuner[*mockMessage]) *Batch[*mockMessage] {
				// Always return a batch smaller than measurement setting to trigger skips
				batch := &Batch[*mockMessage]{}
				batch.totalBytes = int(bbt.measurementSetting.value) - 1
				return batch
			},

			wantCandidate: nil,
			wantConverged: false,
			wantErr:       nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			batchBytesTuner, err := newBatchBytesTuner(AutoTuneConfig{
				Enabled:              true,
				MinBatchBytes:        1,
				MaxBatchBytes:        100,
				ConvergenceThreshold: 0.01,
			}, noopSendFn, testLogger)
			require.NoError(t, err)
			defer batchBytesTuner.close()

			batchBytesTuner.batchBytesToleranceFactor = 0.0 // No tolerance for this test
			batchBytesTuner.calculateThroughputFn = tc.calculateThroughputFn
			batchBytesTuner.minSamples = 1 // reduce min samples for test speed

			ctx := context.Background()
			doneChan := make(chan struct{}, 1)
			go func() {
				defer close(doneChan)
				for range 10 {
					batchBytesTuner.sendBatch(ctx, tc.batch(batchBytesTuner))
				}
				doneChan <- struct{}{}
			}()

			select {
			case <-time.After(10 * time.Second):
				t.Error("batch tuning took too long")
				return
			case <-doneChan:
				require.Equal(t, tc.wantConverged, batchBytesTuner.hasConverged())
				if tc.wantCandidate == nil {
					require.Nil(t, batchBytesTuner.candidateSetting)
				} else {
					require.Equal(t, tc.wantCandidate.value, batchBytesTuner.candidateSetting.value)
					require.Len(t, batchBytesTuner.candidateSetting.throughputs, len(tc.wantCandidate.throughputs))
					require.InDelta(t, tc.wantCandidate.avgThroughput, batchBytesTuner.candidateSetting.avgThroughput, 5)
					require.Equal(t, tc.wantCandidate.direction, batchBytesTuner.candidateSetting.direction)
				}

				return
			}
		})
	}
}

// TestBatchBytesTuner_concurrentAccess exercises the tuner from two goroutines
// concurrently (the writer goroutine driving sendBatch, and the batching
// goroutine reading currentMaxBatchBytes) to ensure the shared state is
// properly synchronised. Run with -race to detect data races.
func TestBatchBytesTuner_concurrentAccess(t *testing.T) {
	t.Parallel()

	testLogger := zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "error",
	}))

	sendFn := func(ctx context.Context, b *Batch[*mockMessage]) error { return nil }

	tuner, err := newBatchBytesTuner(AutoTuneConfig{
		Enabled:              true,
		MinBatchBytes:        1,
		MaxBatchBytes:        1000,
		ConvergenceThreshold: 0.01,
	}, sendFn, testLogger)
	require.NoError(t, err)
	defer tuner.close()

	tuner.batchBytesToleranceFactor = 0.0
	tuner.minSamples = 1
	tuner.calculateThroughputFn = func(duration time.Duration, batchBytes int64) float64 {
		optimal := int64(500)
		distance := float64(batchBytes - optimal)
		throughput := 1000.0 - (distance * distance * 0.001)
		if throughput < 0 {
			return 0
		}
		return throughput
	}

	ctx := context.Background()
	var wg sync.WaitGroup

	// writer goroutine: drives the tuning by sending batches sized to the
	// current measurement setting.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 500 {
			batch := &Batch[*mockMessage]{}
			batch.totalBytes = int(tuner.currentMaxBatchBytes(1000))
			_ = tuner.sendBatch(ctx, batch)
		}
	}()

	// reader goroutines: mimic the batching goroutine reading the current max
	// batch bytes.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 500 {
				_ = tuner.currentMaxBatchBytes(1000)
			}
		}()
	}

	wg.Wait()
}

// TestBatchBytesTuner_failedSendNotRecorded verifies that failed sends do not
// contribute throughput measurements to the tuning signal.
func TestBatchBytesTuner_failedSendNotRecorded(t *testing.T) {
	t.Parallel()

	testLogger := zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "error",
	}))

	errSend := errors.New("send failed")
	sendFn := func(ctx context.Context, b *Batch[*mockMessage]) error { return errSend }

	tuner, err := newBatchBytesTuner(AutoTuneConfig{
		Enabled:              true,
		MinBatchBytes:        1,
		MaxBatchBytes:        100,
		ConvergenceThreshold: 0.01,
	}, sendFn, testLogger)
	require.NoError(t, err)

	tuner.batchBytesToleranceFactor = 0.0
	tuner.minSamples = 1
	tuner.calculateThroughputFn = func(duration time.Duration, batchBytes int64) float64 { return 1000.0 }

	batch := &Batch[*mockMessage]{}
	batch.totalBytes = int(tuner.measurementSetting.value)

	// A failing send must return the error and must not record a measurement.
	for range 5 {
		require.ErrorIs(t, tuner.sendBatch(context.Background(), batch), errSend)
	}

	require.Empty(t, tuner.measurementSetting.throughputs)
	require.Nil(t, tuner.candidateSetting)
}

func Test_calculateThroughput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		duration   time.Duration
		batchBytes int64
		wantResult float64
	}{
		{
			name:       "normal case",
			duration:   1 * time.Second,
			batchBytes: 1000,
			wantResult: 1000.0,
		},
		{
			name:       "half second duration",
			duration:   500 * time.Millisecond,
			batchBytes: 1000,
			wantResult: 2000.0,
		},
		{
			name:       "zero duration",
			duration:   0,
			batchBytes: 1000,
			wantResult: 0.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := calculateThroughput(tc.duration, tc.batchBytes)
			require.InDelta(t, tc.wantResult, result, 0.001)
		})
	}
}
