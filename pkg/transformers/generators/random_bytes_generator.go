// SPDX-License-Identifier: Apache-2.0

package generators

import (
	"sync/atomic"
	"time"

	greenmaskgenerators "github.com/eminano/greenmask/pkg/generators"
)

// seedSequence makes sure generator instances created concurrently or within
// the same clock tick don't share a seed and produce identical streams.
var seedSequence atomic.Int64

// NewRandomBytesGenerator returns a random bytes generator that is safe for
// concurrent use.
func NewRandomBytesGenerator(size int) Generator {
	g, _ := newPooledGenerator(size, func() (greenmaskgenerators.Generator, error) {
		return greenmaskgenerators.NewRandomBytes(time.Now().UnixNano()+seedSequence.Add(1), size), nil
	})
	return g
}
