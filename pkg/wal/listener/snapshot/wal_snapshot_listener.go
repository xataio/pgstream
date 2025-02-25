// SPDX-License-Identifier: Apache-2.0

package snapshot

import "context"

type Generator interface {
	CreateSnapshot(context.Context) error
	Close() error
}
