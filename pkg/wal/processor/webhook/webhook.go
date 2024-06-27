// SPDX-License-Identifier: Apache-2.0

package webhook

import "github.com/xataio/pgstream/pkg/wal"

type Payload struct {
	Data *wal.Data
}
