// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
)

// arrayTransformerRow is the shape both paths are read back into. A NULL array
// arrives as a nil slice, and a NULL element as a nil pointer, so the two are
// distinguishable from an empty array and from the string "NULL".
type arrayTransformerRow struct {
	id     int
	emails []*string
	scores []int32
	tags   []string
}

const (
	arrayTransformerTableDDL = `CREATE TABLE %s(
		id     int PRIMARY KEY,
		emails text[],
		scores int4[],
		tags   text[])`

	// the first row carries an element with a comma, one with a quote and one
	// with a backslash, so a transform that split the literal on commas, or
	// that dropped pgx's quoting, cannot round trip it
	arrayTransformerInsert = `INSERT INTO %s(id, emails, scores, tags) VALUES
		(1, ARRAY['a,b', 'c"d', E'e\\f'], ARRAY[1,2,3], ARRAY['x','y']),
		(2, NULL, ARRAY[]::int4[], NULL),
		(3, ARRAY[NULL, 'NULL'], ARRAY[7], ARRAY['z'])`
)

func arrayTransformerRules(table string) []transformer.TableRules {
	return []transformer.TableRules{
		{
			Schema: "public",
			Table:  table,
			ColumnRules: map[string]transformer.TransformerRules{
				// appends to each element, so the assertion pins both the
				// order and the exact element text
				"emails": {
					Name:       "template",
					Parameters: map[string]any{"template": "{{ .GetValue }}!"},
				},
				// greenmask_integer has no string case in its type switch, so
				// this rule only succeeds if the element reaches it as an
				// integer on both paths
				"scores": {
					Name:       "greenmask_integer",
					Parameters: map[string]any{"min_value": 1000, "max_value": 2000},
				},
				"tags": {
					Name:         "literal_string",
					Parameters:   map[string]any{"literal": "redacted"},
					ArrayOptions: &transformer.ArrayOptions{Generator: "random", MinCount: intPtr(2), MaxCount: intPtr(2)},
				},
			},
		},
	}
}

func intPtr(i int) *int { return &i }

func fetchArrayTransformerRows(ctx context.Context, conn pglib.Querier, table string) ([]arrayTransformerRow, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT id, emails, scores, tags FROM %s ORDER BY id", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []arrayTransformerRow{}
	for rows.Next() {
		var row arrayTransformerRow
		if err := rows.Scan(&row.id, &row.emails, &row.scores, &row.tags); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func requireArrayTransformerRows(t *testing.T, got []arrayTransformerRow) {
	t.Helper()
	require.Len(t, got, 3)

	// the transformer ran once per element, in order, and pgx requoted the
	// elements that need it
	require.Equal(t, []*string{strPtr("a,b!"), strPtr(`c"d!`), strPtr(`e\f!`)}, got[0].emails)
	// a NULL element stays NULL and the transformer is not called on it, while
	// the string "NULL" is transformed like any other element
	require.Equal(t, []*string{nil, strPtr("NULL!")}, got[2].emails)
	// a NULL array stays NULL
	require.Nil(t, got[1].emails)

	// the length is preserved, and every source score is outside the
	// configured range, so a rule that never ran would fail here
	require.Len(t, got[0].scores, 3)
	require.Len(t, got[2].scores, 1)
	for _, row := range got {
		for _, score := range row.scores {
			require.GreaterOrEqual(t, score, int32(1000))
			require.LessOrEqual(t, score, int32(2000))
		}
	}
	// an empty array stays empty rather than becoming NULL
	require.NotNil(t, got[1].scores)
	require.Empty(t, got[1].scores)

	// the random generator emits the configured count, whatever the source
	// length, and never invents an element for an empty or NULL array
	require.Equal(t, []string{"redacted", "redacted"}, got[0].tags)
	require.Equal(t, []string{"redacted", "redacted"}, got[2].tags)
	require.Nil(t, got[1].tags)
}

func strPtr(s string) *string { return &s }

// Test_SnapshotToPostgres_ArrayColumnTransformer covers the snapshot path,
// where an array column reaches the transformer as a decoded slice.
func Test_SnapshotToPostgres_ArrayColumnTransformer(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testTable := "array_transformer_snapshot"
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(arrayTransformerTableDDL, testTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(arrayTransformerInsert, testTable))

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{testTable}),
		Processor: testPostgresProcessorCfg(withTransformerRules(arrayTransformerRules(testTable))),
	}
	initStream(t, ctx, snapshotPGURL)
	runSnapshot(t, ctx, cfg)

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	requireArrayTransformerRows(t, waitForArrayTransformerRows(t, ctx, targetConn, testTable))
}

// Test_PostgresToPostgres_ArrayColumnTransformer covers the replication path,
// where an array column reaches the transformer as its postgres literal. The
// same rules must produce the same shapes on both paths.
func Test_PostgresToPostgres_ArrayColumnTransformer(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// the replication slot has to exist before the table is created, so that
	// the DDL that creates it on the target is replayed from the slot
	listenerCfg := testPostgresListenerCfg(t)

	testTable := "array_transformer_replication"
	execQuery(t, ctx, fmt.Sprintf(arrayTransformerTableDDL, testTable))

	processorCfg := testPostgresProcessorCfgWithTransformer(pgurl)
	processorCfg.Transformer = &transformer.Config{TransformerRules: arrayTransformerRules(testTable)}
	runStream(t, ctx, &stream.Config{
		Listener:  listenerCfg,
		Processor: processorCfg,
	})

	execQuery(t, ctx, fmt.Sprintf(arrayTransformerInsert, testTable))

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	requireArrayTransformerRows(t, waitForArrayTransformerRows(t, ctx, targetConn, testTable))
}

// waitForArrayTransformerRows polls only for arrival. The assertions run on
// the test's own goroutine afterwards, so an intermediate state cannot fail
// the whole run.
func waitForArrayTransformerRows(t *testing.T, ctx context.Context, conn pglib.Querier, table string) []arrayTransformerRow {
	t.Helper()

	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			t.Fatalf("timeout waiting for postgres sync of transformed array columns in %s", table)
			return nil
		case <-ticker.C:
			rows, err := fetchArrayTransformerRows(ctx, conn, table)
			if err == nil && len(rows) == 3 {
				return rows
			}
		}
	}
}
