// SPDX-License-Identifier: Apache-2.0

package generators

import (
	greenmaskgenerators "github.com/eminano/greenmask/pkg/generators"
)

func NewDeterministicBytesGenerator(size int) (Generator, error) {
	return greenmaskgenerators.GetHashBytesGen([]byte{}, size)
}
