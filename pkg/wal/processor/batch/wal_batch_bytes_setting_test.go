// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBatchBytesSetting_IsWithinTolerance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		setting         *batchBytesSetting
		batchBytes      int64
		toleranceFactor float64
		want            bool
	}{
		{
			name:            "nil setting returns false",
			setting:         nil,
			batchBytes:      100,
			toleranceFactor: 0.1,
			want:            false,
		},
		{
			name:            "exact match returns true",
			setting:         &batchBytesSetting{value: 1000},
			batchBytes:      1000,
			toleranceFactor: 0.1,
			want:            true,
		},
		{
			name:            "within lower tolerance returns true",
			setting:         &batchBytesSetting{value: 1000},
			batchBytes:      950,
			toleranceFactor: 0.1,
			want:            true,
		},
		{
			name:            "within upper tolerance returns true",
			setting:         &batchBytesSetting{value: 1000},
			batchBytes:      1050,
			toleranceFactor: 0.1,
			want:            true,
		},
		{
			name:            "at lower boundary returns true",
			setting:         &batchBytesSetting{value: 1000},
			batchBytes:      900,
			toleranceFactor: 0.1,
			want:            true,
		},
		{
			name:            "at upper boundary returns true",
			setting:         &batchBytesSetting{value: 1000},
			batchBytes:      1100,
			toleranceFactor: 0.1,
			want:            true,
		},
		{
			name:            "below lower tolerance returns false",
			setting:         &batchBytesSetting{value: 1000},
			batchBytes:      899,
			toleranceFactor: 0.1,
			want:            false,
		},
		{
			name:            "above upper tolerance returns false",
			setting:         &batchBytesSetting{value: 1000},
			batchBytes:      1101,
			toleranceFactor: 0.1,
			want:            false,
		},
		{
			name:            "zero tolerance factor - exact match only",
			setting:         &batchBytesSetting{value: 1000},
			batchBytes:      1000,
			toleranceFactor: 0.0,
			want:            true,
		},
		{
			name:            "zero tolerance factor - no match",
			setting:         &batchBytesSetting{value: 1000},
			batchBytes:      1001,
			toleranceFactor: 0.0,
			want:            false,
		},
		{
			name:            "large tolerance factor",
			setting:         &batchBytesSetting{value: 1000},
			batchBytes:      500,
			toleranceFactor: 0.5,
			want:            true,
		},
		{
			name:            "small value with tolerance",
			setting:         &batchBytesSetting{value: 10},
			batchBytes:      9,
			toleranceFactor: 0.1,
			want:            true,
		},
		{
			name:            "zero value with tolerance",
			setting:         &batchBytesSetting{value: 0},
			batchBytes:      0,
			toleranceFactor: 0.1,
			want:            true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.setting.IsWithinTolerance(tt.batchBytes, tt.toleranceFactor)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestBatchBytesSetting_CalculateAverageThroughput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		setting               *batchBytesSetting
		minSamples            int
		expectedAvgThroughput float64
		expectedCoV           float64
	}{
		{
			name:                  "nil setting does nothing",
			setting:               nil,
			minSamples:            3,
			expectedAvgThroughput: 0,
			expectedCoV:           0,
		},
		{
			name: "insufficient samples does nothing",
			setting: &batchBytesSetting{
				throughputs: []float64{100.0, 200.0},
			},
			minSamples:            3,
			expectedAvgThroughput: 0,
			expectedCoV:           0,
		},
		{
			name: "exact minimum samples calculates average",
			setting: &batchBytesSetting{
				throughputs: []float64{100.0, 200.0, 300.0},
			},
			minSamples:            3,
			expectedAvgThroughput: 200.0,
			expectedCoV:           0.5,
		},
		{
			name: "more than minimum samples uses last minSamples",
			setting: &batchBytesSetting{
				throughputs: []float64{50.0, 100.0, 200.0, 300.0, 400.0},
			},
			minSamples:            3,
			expectedAvgThroughput: 300.0,
			expectedCoV:           0.3333333333333333,
		},
		{
			name: "single sample with minSamples 1",
			setting: &batchBytesSetting{
				throughputs: []float64{150.0},
			},
			minSamples:            1,
			expectedAvgThroughput: 150.0,
			expectedCoV:           0,
		},
		{
			name: "identical values have zero coefficient of variation",
			setting: &batchBytesSetting{
				throughputs: []float64{100.0, 100.0, 100.0, 100.0},
			},
			minSamples:            3,
			expectedAvgThroughput: 100.0,
			expectedCoV:           0,
		},
		{
			name: "zero values",
			setting: &batchBytesSetting{
				throughputs: []float64{0.0, 0.0, 0.0},
			},
			minSamples:            3,
			expectedAvgThroughput: 0.0,
			expectedCoV:           math.NaN(),
		},
		{
			name: "mixed positive and zero values",
			setting: &batchBytesSetting{
				throughputs: []float64{0.0, 100.0, 200.0},
			},
			minSamples:            3,
			expectedAvgThroughput: 100.0,
			expectedCoV:           1,
		},
		{
			name: "empty throughputs slice does nothing",
			setting: &batchBytesSetting{
				throughputs: []float64{},
			},
			minSamples:            1,
			expectedAvgThroughput: 0,
			expectedCoV:           0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Store original values to verify they don't change when they shouldn't
			var originalAvg, originalCoV float64
			if tt.setting != nil {
				originalAvg = tt.setting.avgThroughput
				originalCoV = tt.setting.coeficientOfVariation
			}

			tt.setting.calculateAverageThroughput(tt.minSamples)

			if tt.setting == nil {
				return
			}

			if len(tt.setting.throughputs) < tt.minSamples {
				// Values should remain unchanged
				require.Equal(t, originalAvg, tt.setting.avgThroughput)
				require.Equal(t, originalCoV, tt.setting.coeficientOfVariation)
			} else {
				require.InDelta(t, tt.expectedAvgThroughput, tt.setting.avgThroughput, 0.0001)
				require.InDelta(t, tt.expectedCoV, tt.setting.coeficientOfVariation, 0.0001)
			}
		})
	}
}

func TestBatchBytesSetting_IsStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setting *batchBytesSetting
		maxCoV  float64
		want    bool
	}{
		{
			name:    "nil setting returns true",
			setting: nil,
			maxCoV:  0.3,
			want:    true,
		},
		{
			name: "stable coefficient of variation returns true",
			setting: &batchBytesSetting{
				coeficientOfVariation: 0.2,
			},
			maxCoV: 0.3,
			want:   true,
		},
		{
			name: "unstable coefficient of variation returns false",
			setting: &batchBytesSetting{
				coeficientOfVariation: 0.4,
			},
			maxCoV: 0.3,
			want:   false,
		},
		{
			name: "coefficient of variation at boundary returns false",
			setting: &batchBytesSetting{
				coeficientOfVariation: 0.3,
			},
			maxCoV: 0.3,
			want:   false,
		},
		{
			name: "zero coefficient of variation returns true",
			setting: &batchBytesSetting{
				coeficientOfVariation: 0.0,
			},
			maxCoV: 0.3,
			want:   true,
		},
		{
			name: "infinite coefficient of variation returns false",
			setting: &batchBytesSetting{
				coeficientOfVariation: math.Inf(1),
			},
			maxCoV: 0.3,
			want:   false,
		},
		{
			name: "negative infinite coefficient of variation returns false",
			setting: &batchBytesSetting{
				coeficientOfVariation: math.Inf(-1),
			},
			maxCoV: 0.3,
			want:   false,
		},
		{
			name: "NaN coefficient of variation returns false",
			setting: &batchBytesSetting{
				coeficientOfVariation: math.NaN(),
			},
			maxCoV: 0.3,
			want:   false,
		},
		{
			name: "very small maxCoV threshold",
			setting: &batchBytesSetting{
				coeficientOfVariation: 0.01,
			},
			maxCoV: 0.005,
			want:   false,
		},
		{
			name: "very large maxCoV threshold",
			setting: &batchBytesSetting{
				coeficientOfVariation: 0.5,
			},
			maxCoV: 1.0,
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.setting.isStable(tt.maxCoV)
			require.Equal(t, tt.want, got)
		})
	}
}
