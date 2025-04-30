// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"time"
)

type mockRow struct {
	lsn    string
	lag    int64
	exists bool
	scanFn func(args ...any) error
}

func (m *mockRow) Scan(args ...any) error {
	if m.scanFn != nil {
		return m.scanFn(args...)
	}

	if len(args) != 1 {
		return fmt.Errorf("expected 1 argument, got %d", len(args))
	}

	switch arg := args[0].(type) {
	case *string:
		*arg = m.lsn
	case *int64:
		*arg = m.lag
	case *bool:
		*arg = m.exists
	default:
		return fmt.Errorf("unexpected argument type in scan: %T", args[0])
	}

	return nil
}

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
