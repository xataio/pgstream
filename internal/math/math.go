// SPDX-License-Identifier: Apache-2.0

package math

import (
	"math"
)

// Median calculates the median value between min and max.
func Median(min, max int64) int64 {
	return min + (max-min)/2
}

// StandardDeviation calculates the sample standard deviation of a slice of
// float64 values using Bessel's correction (dividing by n-1).
func StandardDeviation(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if len(values) == 1 {
		return 0
	}

	// Calculate mean
	var total float64
	for _, v := range values {
		total += v
	}
	mean := total / float64(len(values))

	// Calculate variance using Bessel's correction (n-1)
	var sumSquaredDiff float64
	for _, v := range values {
		diff := v - mean
		sumSquaredDiff += diff * diff
	}
	variance := sumSquaredDiff / float64(len(values)-1)

	// Standard deviation is square root of variance
	return math.Sqrt(variance)
}

// Average calculates the average of a slice of float64 values.
func Average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var total float64
	for _, v := range values {
		total += v
	}
	return total / float64(len(values))
}

// Percentile calculates the given percentile of a slice of float64 values.
func Percentile(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}

	// Create a sorted copy
	sorted := make([]float64, len(values))
	copy(sorted, values)

	// Simple bubble sort (good enough for small datasets)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	// Calculate percentile index
	index := percentile * float64(len(sorted)-1)
	lower := int(index)
	upper := lower + 1

	if upper >= len(sorted) {
		return sorted[len(sorted)-1]
	}

	// Linear interpolation between two closest values
	weight := index - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

// CoefficientOfVariation calculates the coefficient of variation (CV)
// which is the ratio of standard deviation to mean. CV is a normalized measure
// of dispersion that is useful for comparing variability across different scales.
// Uses sample standard deviation (dividing by n-1).
// Returns NaN when the mean is zero, as CV is mathematically undefined in this case.
func CoefficientOfVariation(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}

	mean := Average(values)
	if mean == 0 {
		return math.NaN()
	}

	stdDev := StandardDeviation(values)
	return stdDev / mean
}
