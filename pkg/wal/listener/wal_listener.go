// SPDX-License-Identifier: Apache-2.0

package listener

import "context"

type Listener interface {
	Listen(ctx context.Context) error
	Close() error
}
