// SPDX-License-Identifier: Apache-2.0

package preflight

import "fmt"

// prettySize renders a byte count in the same format as pg_size_pretty. Size
// checks report bytes through Details and render them here, so this is the one
// place where a byte count becomes text. Postgres steps up to the next unit
// when the value reaches 10 times that unit, and rounds half away from zero at
// each step (see pg_size_pretty in src/backend/utils/adt/dbsize.c).
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

// roundedDiv divides value by divisor and rounds half away from zero. This
// matches the rounding that Postgres applies at each unit step.
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
