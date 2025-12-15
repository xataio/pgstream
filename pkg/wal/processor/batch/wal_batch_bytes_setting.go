// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"fmt"
	"math"

	mathlib "github.com/xataio/pgstream/internal/math"
)

// batchBytesSetting represents a specific setting for the batch bytes size. It
// tracks the throughput measurements collected for this setting, calculates
// average throughput and coefficient of variation, and determines stability.
type batchBytesSetting struct {
	value                 int64
	throughputs           []float64
	avgThroughput         float64
	coeficientOfVariation float64
	direction             direction
	skippedCount          uint
}

type direction string

const (
	directionLeft  direction = "left"
	directionRight direction = "right"
)

func newBatchBytesSetting(value int64, direction direction) *batchBytesSetting {
	return &batchBytesSetting{
		value:     value,
		direction: direction,
	}
}

func (s *batchBytesSetting) String() string {
	if s == nil {
		return "<nil>"
	}
	return fmt.Sprintf("[value: %d, avg throughput: %.2fb/s, coeficient of variation: %.2f, sample count: %d, direction: %s]", s.value, s.avgThroughput, s.coeficientOfVariation, len(s.throughputs), s.direction)
}

func (s *batchBytesSetting) IsWithinTolerance(batchBytes int64, toleranceFactor float64) bool {
	if s == nil {
		return false
	}
	bytesTolerance := int64(float64(s.value) * toleranceFactor)
	return batchBytes >= (s.value-bytesTolerance) && batchBytes <= (s.value+bytesTolerance)
}

func (s *batchBytesSetting) addThroughput(throughput float64) {
	if s == nil {
		return
	}
	s.throughputs = append(s.throughputs, throughput)
}

func (s *batchBytesSetting) hasMinSamples(minSamples int) bool {
	return s != nil && len(s.throughputs) >= minSamples
}

func (s *batchBytesSetting) hasMaxSamples(maxSamples int) bool {
	return s != nil && len(s.throughputs) >= maxSamples
}

func (s *batchBytesSetting) calculateAverageThroughput(minSamples int) {
	if s == nil || len(s.throughputs) < minSamples {
		return
	}

	// Consider only the last minSamples for average and CoV calculation
	lastMinSamples := s.throughputs[len(s.throughputs)-minSamples:]

	var total float64
	for _, v := range lastMinSamples {
		total += v
	}

	s.avgThroughput = total / float64(len(lastMinSamples))
	s.coeficientOfVariation = mathlib.CoefficientOfVariation(lastMinSamples)
}

func (s *batchBytesSetting) isStable(maxCoV float64) bool {
	if s == nil {
		return true // not enough data to determine stability
	}

	// Use the coefficient of variation that was already calculated
	// Consider stable if coefficient of variation is less than 30%
	return !math.IsInf(s.coeficientOfVariation, 0) &&
		!math.IsNaN(s.coeficientOfVariation) &&
		s.coeficientOfVariation < maxCoV
}
