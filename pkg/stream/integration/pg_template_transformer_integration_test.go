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

// The template transformer accepts every column type. These tests apply it to
// one column of each common type and check the rendered value lands in the
// target with the same result from a snapshot and from replication. The two
// paths hand the template different Go types for the same column (a snapshot
// row carries pgx types, a replicated row carries the wal2json text), so a
// template that renders correctly from one source must also render correctly
// from the other.

func templateTransformerCreateTable(table string) string {
	return fmt.Sprintf(`CREATE TABLE %s(
		id      integer PRIMARY KEY,
		small   smallint,
		i       integer,
		big     bigint,
		amount  numeric(12,4),
		ratio   double precision,
		active  boolean,
		born    date,
		created timestamptz,
		uid     uuid,
		meta    jsonb,
		name    text,
		t       time,
		iv      interval,
		raw     bytea,
		tags    text[],
		ints    int4[],
		bits    bit(4),
		r       int4range,
		dr      daterange,
		tsv     tsvector,
		hs      hstore,
		xm      xml
	)`, table)
}

func templateTransformerInsert(table string) string {
	return fmt.Sprintf(`INSERT INTO %s(id, small, i, big, amount, ratio, active, born, created, uid, meta, name,
		t, iv, raw, tags, ints, bits, r, dr, tsv, hs, xm) VALUES
		(1, 7, 42, 9007199254740993, 1234.5678, 3.25, true, '2024-02-29', '2024-02-29 10:34:56+00', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"k": 1}', 'hello',
		'12:34:56', '1 day 02:03:04', '\xdeadbeef', '{a,"b,c"}', '{1,2}', B'1010', '[1,10)', '[2024-01-01,2024-02-01)', 'a fat cat', 'k=>v', '<a>x</a>')`, table)
}

func templateTransformerRules(table string) []transformer.TableRules {
	templateRule := func(tmpl string) transformer.TransformerRules {
		return transformer.TransformerRules{Name: "template", Parameters: map[string]any{"template": tmpl}}
	}
	return []transformer.TableRules{{
		Schema: "public",
		Table:  table,
		ColumnRules: map[string]transformer.TransformerRules{
			"small":   templateRule(`{{ add .GetValue 1 }}`),
			"i":       templateRule(`{{ mul .GetValue 2 }}`),
			"big":     templateRule(`{{ add .GetValue 1 }}`),
			"amount":  templateRule(`{{ .GetValue }}`),
			"ratio":   templateRule(`{{ mulf .GetValue 2 }}`),
			"active":  templateRule(`{{ not .GetValue }}`),
			"born":    templateRule(`{{ .GetValue }}`),
			"created": templateRule(`{{ .GetValue }}`),
			"uid":     templateRule(`{{ .GetValue }}`),
			"meta":    templateRule(`{{ .GetValue }}`),
			"name":    templateRule(`{{ upper .GetValue }}`),
			// the remaining columns echo the value: from a snapshot they
			// reach the template as pgx types, which must render as the
			// postgres text replication delivers for the same column
			"t":    templateRule(`{{ .GetValue }}`),
			"iv":   templateRule(`{{ .GetValue }}`),
			"raw":  templateRule(`{{ .GetValue }}`),
			"tags": templateRule(`{{ .GetValue }}`),
			"ints": templateRule(`{{ .GetValue }}`),
			"bits": templateRule(`{{ .GetValue }}`),
			"r":    templateRule(`{{ .GetValue }}`),
			"dr":   templateRule(`{{ .GetValue }}`),
			"tsv":  templateRule(`{{ .GetValue }}`),
			"hs":   templateRule(`{{ .GetValue }}`),
			"xm":   templateRule(`{{ .GetValue }}`),
		},
	}}
}

type templateTransformerRow struct {
	id      int
	small   *int16
	i       *int32
	big     *int64
	amount  *string
	ratio   *float64
	active  *bool
	born    *time.Time
	created *time.Time
	uid     *string
	meta    *string
	name    *string
	t       *string
	iv      *string
	raw     []byte
	tags    *string
	ints    *string
	bits    *string
	r       *string
	dr      *string
	tsv     *string
	hs      *string
	xm      *string
}

// waitForTemplateTransformerRow polls the target until the transformed row is
// readable and then asserts on it once, on the test's own goroutine.
func waitForTemplateTransformerRow(t *testing.T, ctx context.Context, table string) {
	t.Helper()

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	query := fmt.Sprintf(`SELECT id, small, i, big, amount::text, ratio, active, born, created, uid::text, meta::text, name,
		t::text, iv::text, raw, tags::text, ints::text, bits::text, r::text, dr::text, tsv::text, hs::text, xm::text
		FROM %s WHERE id = 1`, table)

	var got *templateTransformerRow
	require.Eventually(t, func() bool {
		var r templateTransformerRow
		dst := []any{
			&r.id, &r.small, &r.i, &r.big, &r.amount, &r.ratio, &r.active, &r.born, &r.created, &r.uid, &r.meta, &r.name,
			&r.t, &r.iv, &r.raw, &r.tags, &r.ints, &r.bits, &r.r, &r.dr, &r.tsv, &r.hs, &r.xm,
		}
		if err := targetConn.QueryRow(ctx, dst, query); err != nil {
			return false
		}
		got = &r
		return true
	}, 30*time.Second, time.Second, "timeout waiting for the template transformed row")

	// a template that errors is nulled by the default on_error policy, so a
	// nil column here means the template did not render from this source
	require.NotNil(t, got.small, "small")
	require.NotNil(t, got.i, "i")
	require.NotNil(t, got.big, "big")
	require.NotNil(t, got.amount, "amount")
	require.NotNil(t, got.ratio, "ratio")
	require.NotNil(t, got.active, "active")
	require.NotNil(t, got.born, "born")
	require.NotNil(t, got.created, "created")
	require.NotNil(t, got.uid, "uid")
	require.NotNil(t, got.meta, "meta")
	require.NotNil(t, got.name, "name")
	require.NotNil(t, got.t, "t")
	require.NotNil(t, got.iv, "iv")
	require.NotNil(t, got.raw, "raw")
	require.NotNil(t, got.tags, "tags")
	require.NotNil(t, got.ints, "ints")
	require.NotNil(t, got.bits, "bits")
	require.NotNil(t, got.r, "r")
	require.NotNil(t, got.dr, "dr")
	require.NotNil(t, got.tsv, "tsv")
	require.NotNil(t, got.hs, "hs")
	require.NotNil(t, got.xm, "xm")

	require.Equal(t, int16(8), *got.small)
	require.Equal(t, int32(84), *got.i)
	require.Equal(t, int64(9007199254740994), *got.big)
	require.Equal(t, "1234.5678", *got.amount)
	require.Equal(t, 6.5, *got.ratio)
	require.Equal(t, false, *got.active)
	require.Equal(t, time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC), got.born.UTC())
	require.Equal(t, time.Date(2024, 2, 29, 10, 34, 56, 0, time.UTC), got.created.UTC())
	require.Equal(t, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", *got.uid)
	require.JSONEq(t, `{"k": 1}`, *got.meta)
	require.Equal(t, "HELLO", *got.name)
	require.Equal(t, "12:34:56", *got.t)
	require.Equal(t, "1 day 02:03:04", *got.iv)
	require.Equal(t, []byte{0xde, 0xad, 0xbe, 0xef}, got.raw)
	require.Equal(t, `{a,"b,c"}`, *got.tags)
	require.Equal(t, "{1,2}", *got.ints)
	require.Equal(t, "1010", *got.bits)
	require.Equal(t, "[1,10)", *got.r)
	require.Equal(t, "[2024-01-01,2024-02-01)", *got.dr)
	require.Equal(t, "'a' 'cat' 'fat'", *got.tsv)
	require.Equal(t, `"k"=>"v"`, *got.hs)
	require.Equal(t, "<a>x</a>", *got.xm)
}

func Test_SnapshotToPostgres_TemplateTransformerColumnTypes(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	run := func(t *testing.T, suffix string, opts ...option) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testTable := fmt.Sprintf("template_transformer_types_%s", suffix)
		// the schema dump is filtered to the table, so the target needs
		// the extension before the restore
		execQueryWithURL(t, ctx, snapshotPGURL, "CREATE EXTENSION IF NOT EXISTS hstore")
		execQueryWithURL(t, ctx, targetPGURL, "CREATE EXTENSION IF NOT EXISTS hstore")
		execQueryWithURL(t, ctx, snapshotPGURL, templateTransformerCreateTable(testTable))
		execQueryWithURL(t, ctx, snapshotPGURL, templateTransformerInsert(testTable))

		cfg := &stream.Config{
			Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{testTable}),
			Processor: testPostgresProcessorCfg(append(opts, withTransformerRules(templateTransformerRules(testTable)))...),
		}
		initStream(t, ctx, snapshotPGURL)
		runSnapshot(t, ctx, cfg)

		waitForTemplateTransformerRow(t, ctx, testTable)
	}

	t.Run("bulk ingest", func(t *testing.T) {
		run(t, "bulk", withBulkIngestionEnabled())
	})
	t.Run("batch writer", func(t *testing.T) {
		run(t, "batch")
	})
}

func Test_PostgresToPostgres_TemplateTransformerColumnTypes(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testTable := "pg2pg_template_transformer_types"

	// the listener config creates the replication slot, so it has to exist
	// before the table does or the DDL lands ahead of the slot
	listenerCfg := testPostgresListenerCfg(t)
	execQuery(t, ctx, "CREATE EXTENSION IF NOT EXISTS hstore")
	execQueryWithURL(t, ctx, targetPGURL, "CREATE EXTENSION IF NOT EXISTS hstore")
	execQuery(t, ctx, templateTransformerCreateTable(testTable))
	// this pipeline has no injector, so DDL is not replicated; the target
	// table has to exist before the first row arrives
	execQueryWithURL(t, ctx, targetPGURL, templateTransformerCreateTable(testTable))

	cfg := &stream.Config{
		Listener:  listenerCfg,
		Processor: testPostgresProcessorCfg(withTransformerRules(templateTransformerRules(testTable))),
	}
	runStream(t, ctx, cfg)

	execQuery(t, ctx, templateTransformerInsert(testTable))

	waitForTemplateTransformerRow(t, ctx, testTable)
}
