// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"context"
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
