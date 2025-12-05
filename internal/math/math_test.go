// SPDX-License-Identifier: Apache-2.0

package math

import (
	"math"
	"testing"
)

func TestMedian(t *testing.T) {
	tests := []struct {
		name     string
		min, max int64
		expected int64
	}{
		{"positive range", 10, 20, 15},
		{"negative range", -20, -10, -15},
		{"mixed range", -10, 10, 0},
		{"same values", 5, 5, 5},
		{"large numbers", 1000000, 2000000, 1500000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Median(tt.min, tt.max)
			if result != tt.expected {
				t.Errorf("Median(%d, %d) = %d; want %d", tt.min, tt.max, result, tt.expected)
			}
		})
	}
}

func TestStandardDeviation(t *testing.T) {
	tests := []struct {
		name      string
		values    []float64
		expected  float64
		tolerance float64
	}{
		{"empty slice", []float64{}, 0, 0},
		{"single value", []float64{5}, 0, 0},
		{"identical values", []float64{3, 3, 3, 3}, 0, 0},
		{"simple case", []float64{1, 2, 3, 4, 5}, 1.5811388300841898, 1e-10},
		{"negative values", []float64{-2, -1, 0, 1, 2}, 1.5811388300841898, 1e-10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StandardDeviation(tt.values)
			if math.Abs(result-tt.expected) > tt.tolerance {
				t.Errorf("StandardDeviation(%v) = %f; want %f", tt.values, result, tt.expected)
			}
		})
	}
}

func TestAverage(t *testing.T) {
	tests := []struct {
		name     string
		values   []float64
		expected float64
	}{
		{"empty slice", []float64{}, 0},
		{"single value", []float64{42}, 42},
		{"positive values", []float64{1, 2, 3, 4, 5}, 3},
		{"negative values", []float64{-1, -2, -3}, -2},
		{"mixed values", []float64{-10, 0, 10}, 0},
		{"decimal values", []float64{1.5, 2.5, 3.0}, 2.333333333333333},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Average(tt.values)
			if math.Abs(result-tt.expected) > 1e-10 {
				t.Errorf("Average(%v) = %f; want %f", tt.values, result, tt.expected)
			}
		})
	}
}

func TestPercentile(t *testing.T) {
	tests := []struct {
		name       string
		values     []float64
		percentile float64
		expected   float64
		tolerance  float64
	}{
		{"empty slice", []float64{}, 0.5, 0, 0},
		{"single value", []float64{10}, 0.5, 10, 0},
		{"50th percentile", []float64{1, 2, 3, 4, 5}, 0.5, 3, 1e-10},
		{"0th percentile", []float64{1, 2, 3, 4, 5}, 0, 1, 1e-10},
		{"100th percentile", []float64{1, 2, 3, 4, 5}, 1, 5, 1e-10},
		{"25th percentile", []float64{1, 2, 3, 4, 5}, 0.25, 2, 1e-10},
		{"75th percentile", []float64{1, 2, 3, 4, 5}, 0.75, 4, 1e-10},
		{"unsorted input", []float64{5, 1, 3, 2, 4}, 0.5, 3, 1e-10},
		{"duplicate values", []float64{1, 1, 2, 2, 3}, 0.5, 2, 1e-10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Percentile(tt.values, tt.percentile)
			if math.Abs(result-tt.expected) > tt.tolerance {
				t.Errorf("Percentile(%v, %f) = %f; want %f", tt.values, tt.percentile, result, tt.expected)
			}
		})
	}
}

func TestCoefficientOfVariation(t *testing.T) {
	tests := []struct {
		name      string
		values    []float64
		expected  float64
		tolerance float64
	}{
		{"empty slice", []float64{}, 0, 0},
		{"single value", []float64{5}, 0, 0},
		{"identical values", []float64{3, 3, 3, 3}, 0, 0},
		{"zero mean", []float64{-2, -1, 0, 1, 2}, math.Inf(1), 0},
		{"simple case", []float64{2, 4, 6, 8}, 0.5163977794943222, 1e-10},
		{"high variability", []float64{1, 10, 100}, 1.4795908671395625, 1e-5},
		{"low variability", []float64{99, 100, 101}, 0.01, 1e-10},
		{"negative values", []float64{-10, -5, -15}, -0.5, 1e-10},
		{"mixed positive/negative", []float64{-5, -3, -1, 1, 3, 5}, math.Inf(1), 0},
		{"decimal values", []float64{1.1, 1.2, 1.3, 1.4, 1.5}, 0.12162570084338147, 1e-5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CoefficientOfVariation(tt.values)
			if math.IsInf(tt.expected, 0) {
				if !math.IsInf(result, 0) {
					t.Errorf("CoefficientOfVariation(%v) = %f; want %f", tt.values, result, tt.expected)
				}
			} else if math.Abs(result-tt.expected) > tt.tolerance {
				t.Errorf("CoefficientOfVariation(%v) = %f; want %f", tt.values, result, tt.expected)
			}
		})
	}
}
