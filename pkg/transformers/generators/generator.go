// SPDX-License-Identifier: Apache-2.0

package generators

type Generator interface {
	Generate([]byte) ([]byte, error)
	Size() int
}
