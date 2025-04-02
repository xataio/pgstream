// SPDX-License-Identifier: Apache-2.0

package generators

import (
	"time"

	greenmaskgenerators "github.com/eminano/greenmask/pkg/generators"
)

func NewRandomBytesGenerator(size int) Generator {
	return greenmaskgenerators.NewRandomBytes(time.Now().UnixNano(), size)
}
