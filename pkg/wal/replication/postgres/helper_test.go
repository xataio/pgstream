// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"time"
)

const (
	testDBName = "test-db"
	testSlot   = "test_slot"
	testLSN    = uint64(7773397064)
	testLSNStr = "1/CF54A048"
)

var (
	errTest = errors.New("oh noes")

	now = time.Now()
)
