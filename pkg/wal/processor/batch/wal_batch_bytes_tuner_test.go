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

	tests := []struct {
		name         string
		throughputFn func(time.Duration, int64) float64

		wantCandidate *batchBytesSetting
		wantConverged bool
		wantErr       error
	}{
		{
			name: "converged after going left",
			throughputFn: func(duration time.Duration, batchBytes int64) float64 {
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

			wantCandidate: &batchBytesSetting{
				value:      20,
				throughput: 1000.0,
				direction:  directionLeft,
			},
			wantConverged: true,
			wantErr:       nil,
		},
		{
			name: "converged to initial value",
			throughputFn: func(duration time.Duration, batchBytes int64) float64 {
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

			wantCandidate: &batchBytesSetting{
				value:      50,
				throughput: 1000.0,
				direction:  "",
			},
			wantConverged: true,
			wantErr:       nil,
		},
		{
			name: "converged after going right",
			throughputFn: func(duration time.Duration, batchBytes int64) float64 {
				// simulate a parabolic throughput curve with peak at 50 bytes
				optimalBatchBytes := int64(81)
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

			wantCandidate: &batchBytesSetting{
				value:      81,
				throughput: 1000.0,
				direction:  directionRight,
			},
			wantConverged: true,
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

			batchBytesTuner.throughputFn = tc.throughputFn

			ctx := context.Background()
			doneChan := make(chan struct{}, 1)
			go func() {
				defer close(doneChan)
				for range 10 {
					batchBytesTuner.sendBatch(ctx, mockBatch)
				}
				doneChan <- struct{}{}
			}()

			select {
			case <-time.After(10 * time.Second):
				t.Error("batch tuning took too long")
				return
			case <-doneChan:
				require.Equal(t, tc.wantConverged, batchBytesTuner.hasConverged())
				require.Equal(t, tc.wantCandidate.value, batchBytesTuner.candidateSetting.value)
				require.InDelta(t, tc.wantCandidate.throughput, batchBytesTuner.candidateSetting.throughput, 5)
				require.Equal(t, tc.wantCandidate.direction, batchBytesTuner.candidateSetting.direction)
				return
			}
		})
	}
}
