// SPDX-License-Identifier: Apache-2.0

package preflight

import "fmt"

// prettySize renders a byte count the way pg_size_pretty does, so a total
// computed in Go reads consistently beside the per-row strings the size
// queries get from Postgres itself.
//
// Postgres steps up a unit once the value reaches 10 times that unit's size,
// and rounds half away from zero on each step (see pg_size_pretty in
// src/backend/utils/adt/dbsize.c).
func prettySize(bytes int64) string {
	const (
		unit  = 1024
		limit = 10 * unit
	)
	if abs(bytes) < limit {
		return fmt.Sprintf("%d bytes", bytes)
	}
	size := bytes
	for _, suffix := range []string{"kB", "MB", "GB", "TB"} {
		size = roundedDiv(size, unit)
		if abs(size) < limit || suffix == "TB" {
			return fmt.Sprintf("%d %s", size, suffix)
		}
	}
	return fmt.Sprintf("%d TB", size)
}

// roundedDiv divides by divisor rounding half away from zero, matching the
// half-up rounding Postgres applies at each unit step.
func roundedDiv(value, divisor int64) int64 {
	half := divisor / 2
	if value < 0 {
		return -((-value + half) / divisor)
	}
	return (value + half) / divisor
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
