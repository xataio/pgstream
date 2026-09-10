// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/xataio/pgstream/pkg/transformers/internal/lookup"
)

// lookupTestValues is what lookup_choice reads in this package's tests
var lookupTestValues = []any{int64(1), int64(2)}

// lookup_choice reads its values while it is being built, and the tables in
// this package build every registered transformer. The seam is installed here
// rather than inside a test because those tests run in parallel and would
// otherwise race each other writing it.
func TestMain(m *testing.M) {
	lookup.NewQuerier = lookup.StubQuerier(pgtype.Int8OID, lookupTestValues, lookup.StubOptions{})
	os.Exit(m.Run())
}
