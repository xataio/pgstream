// SPDX-License-Identifier: Apache-2.0

package generators

import (
	greenmaskgenerators "github.com/eminano/greenmask/pkg/generators"
)

// NewDeterministicBytesGenerator returns a hash based generator that is safe
// for concurrent use. All instances hash with the same (empty) salt, so the
// output only depends on the input.
func NewDeterministicBytesGenerator(size int) (Generator, error) {
	return newPooledGenerator(size, func() (greenmaskgenerators.Generator, error) {
		return greenmaskgenerators.GetHashBytesGen([]byte{}, size)
	})
}
