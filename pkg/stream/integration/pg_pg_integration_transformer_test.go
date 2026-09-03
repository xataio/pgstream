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
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
)

type transformerTestTableRow struct {
	id             int
	name           string
	lastName       string
	email          string
	secondaryEmail string
	address        string
	age            int
	totalPurchases float64
	customerID     string
	birthDate      time.Time
	isActive       bool
	createdAt      int64
	updatedAt      time.Time
	gender         string
}

var createTableQuery = `CREATE TABLE %s(
	id serial primary key,
	name text,
	last_name varchar(255),
	email varchar(255),
	secondary_email varchar(255),
	address text,
	age integer,
	total_purchases double precision,
	customer_id uuid,
	birth_date date,
	is_active bool,
	created_at bigint,
	updated_at timestamp with time zone,
	gender varchar(255));`

func Test_PostgresToPostgres_Transformer(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(t),
		Processor: testPostgresProcessorCfgWithTransformer(pgurl),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testTable := "pg2pg_integration_transformer_test"
	execQuery(t, ctx, fmt.Sprintf(createTableQuery, testTable))

	runStream(t, ctx, cfg)

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	tests := []struct {
		name string
		rows []transformerTestTableRow
	}{
		{
			name: "insert data",
			rows: []transformerTestTableRow{
				{
					id:             1,
					name:           "John",
					lastName:       "Doe",
					email:          "john.doe@example.com",
					secondaryEmail: "john.doe2@example.com",
					address:        "123 Main St",
					age:            30,
					totalPurchases: 1000.50,
					customerID:     "123e4567-e89b-12d3-a456-426655440000",
					birthDate:      time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC),
					isActive:       true,
					createdAt:      1672531200,
					updatedAt:      time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
					gender:         "male",
				},
			},
		},
	}

	nextRowID := 1
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			execQuery(t, ctx, insertQuery(testTable, tc.rows))

			// the serial primary key hands out ids in insert order, so each
			// inserted id maps back to the source row it was built from. The
			// assertions that compare against the source have to use that row,
			// not an arbitrary one.
			insertedRows := []int{}
			sourceByID := map[int]transformerTestTableRow{}
			for _, row := range tc.rows {
				insertedRows = append(insertedRows, nextRowID)
				sourceByID[nextRowID] = row
				nextRowID++
			}

			timer := time.NewTimer(20 * time.Second)
			defer timer.Stop()
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-timer.C:
					cancel()
					t.Error("timeout waiting for postgres sync")
					return
				case <-ticker.C:
					if validateRows(t, ctx, targetConn, insertedRows, testTable, sourceByID) {
						return
					}
				}
			}
		})
	}
}

func insertQuery(table string, rows []transformerTestTableRow) string {
	query := fmt.Sprintf("INSERT INTO %s (name, last_name, email, secondary_email, address, age, total_purchases, customer_id, birth_date, is_active, created_at, updated_at, gender) VALUES", table)
	for i, row := range rows {
		if i > 0 {
			query += ","
		}
		query += fmt.Sprintf(" ('%s', '%s', '%s', '%s', '%s', %d, %f, '%s', '%s', %t, %d, '%s', '%s')",
			row.name, row.lastName, row.email, row.secondaryEmail, row.address, row.age, row.totalPurchases, row.customerID, row.birthDate.Format(time.DateOnly), row.isActive, row.createdAt, row.updatedAt.Format(time.RFC3339), row.gender)
	}
	return query
}

// validateRows checks two things the per-transformer unit tests cannot: that
// each rule was actually applied on the way through the pipeline, and that the
// value it produced is writable into the real postgres column type.
//
// It deliberately does not restate the value ranges the transformer configs
// declare — pkg/transformers has a test per transformer for that, and repeating
// the bounds here only re-asserts that a range is a range. What is kept is the
// subset that discriminates: an assertion the *source* value would fail, so a
// rule that parsed but was never applied cannot slip through. Where no such
// assertion exists for a column, the column is still checked, either against
// the bounds its rule declares or, where the type admits nothing else, by
// having survived the scan into its Go type. Every column carrying a
// transformer rule is covered by one of the three.
//
// sourceByID maps each inserted primary key to the row it was built from, so
// the comparisons against the source use the row that actually produced the
// value rather than whichever row happened to be first.
func validateRows(t *testing.T, ctx context.Context, conn *pglib.Conn, expectedRows []int, table string, sourceByID map[int]transformerTestTableRow) bool {
	selectQuery := fmt.Sprintf("SELECT id, name, last_name, email, secondary_email, address, age, total_purchases, customer_id, birth_date, is_active, created_at, updated_at, gender FROM %s WHERE id IN (", table)
	for i, rowID := range expectedRows {
		if i > 0 {
			selectQuery += " ,"
		}
		selectQuery += fmt.Sprintf("%d", rowID)
	}
	selectQuery += ")"
	rows, err := conn.Query(ctx, selectQuery)
	require.NoError(t, err)
	defer rows.Close()

	rowsFromDB := []transformerTestTableRow{}
	for rows.Next() {
		row := transformerTestTableRow{}
		err := rows.Scan(&row.id, &row.name, &row.lastName, &row.email, &row.secondaryEmail, &row.address, &row.age, &row.totalPurchases, &row.customerID, &row.birthDate, &row.isActive, &row.createdAt, &row.updatedAt, &row.gender)
		require.NoError(t, err)
		rowsFromDB = append(rowsFromDB, row)
	}
	require.NoError(t, rows.Err())

	if len(rowsFromDB) != len(expectedRows) {
		return false
	}

	for _, row := range rowsFromDB {
		source, ok := sourceByID[row.id]
		require.True(t, ok, "row %d was not inserted by this test", row.id)

		// --- the rule was applied: each of these fails on the source value ---

		// masking is deterministic, so the whole output is pinned
		require.Equal(t, "joh****e2@example.com", row.secondaryEmail)
		// the source gender is "male", which is not one of the choices
		require.True(t, row.gender == "M" || row.gender == "F" || row.gender == "None",
			"gender %q is neither a configured choice nor evidence of a rule", row.gender)
		// the source email is 20 characters, past the configured max of 15
		require.LessOrEqual(t, len(row.email), 15)
		// the source total_purchases is 1000.50, above the configured max of 1000
		require.LessOrEqual(t, row.totalPurchases, 1000.0)
		// the source created_at is 1672531200, below the configured minimum
		require.GreaterOrEqual(t, row.createdAt, int64(1741856058))
		// a uuid transformer returning its input would leave the source uuid
		require.NotEqual(t, source.customerID, row.customerID)

		// --- no source-discriminating assertion exists, so the declared
		// bounds stand in ---
		//
		// The source value already satisfies these rules, so passing does not
		// prove the rule ran. What it does catch is a transformer that ran and
		// produced something outside the domain it was configured for.

		require.GreaterOrEqual(t, row.age, 18, "age below the configured minimum")
		require.LessOrEqual(t, row.age, 75, "age above the configured maximum")
		require.LessOrEqual(t, row.createdAt, int64(1741956058), "created_at above the configured maximum")

		// --- the value is writable into the real column type ---
		//
		// Nothing else is checkable for these: the rule declares no bounds, or
		// the type admits every value the rule can produce. The transformed
		// value having survived a scan into its Go type is the assertion — a
		// transformer emitting something postgres rejects fails the insert on
		// the target instead, and the row never arrives to be read here.
		// row.isActive is covered by that scan alone: a bool has no shape left
		// to assert on.

		require.NotEmpty(t, row.customerID, "uuid column")
		require.False(t, row.birthDate.IsZero(), "date column")
		require.False(t, row.updatedAt.IsZero(), "timestamp with time zone column")
		require.NotEmpty(t, row.name, "text column")
		require.NotEmpty(t, row.lastName, "varchar column")
		require.NotEmpty(t, row.address, "text column")
	}
	return true
}

// Test_PostgresToPostgres_NumericTransformer covers the replication half of
// numeric support. wal2json renders a whole numeric as a JSON integer and the
// listener decodes those as int64, so a numeric column reaches the transformer
// as an int64 here and as a pgtype.Numeric during a snapshot. A transformer
// that only accepts the snapshot shape rejects the value, and the default
// on_error policy then nulls the column rather than failing the run, so
// nothing but an assertion on the target catches it.
func Test_PostgresToPostgres_NumericTransformer(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	testTable := "pg2pg_integration_numeric_transformer_test"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	createTable := fmt.Sprintf(
		`CREATE TABLE %s(
			id      serial PRIMARY KEY,
			amount  numeric,
			lat     numeric(9,6),
			dbl     double precision
		)`, testTable)
	execQuery(t, ctx, createTable)
	// this pipeline has no injector, so DDL is not replicated; the target
	// table has to exist before the first row arrives
	execQueryWithURL(t, ctx, targetPGURL, createTable)

	rules := []transformer.TableRules{{
		Schema: "public",
		Table:  testTable,
		ColumnRules: map[string]transformer.TransformerRules{
			"amount": {Name: "greenmask_float", Parameters: map[string]any{
				"generator": "deterministic", "min_value": 0.0, "max_value": 1000.0,
			}},
			"lat": {Name: "greenmask_float", Parameters: map[string]any{
				"generator": "deterministic", "min_value": -90.0, "max_value": 90.0, "precision": 6,
			}},
			"dbl": {Name: "greenmask_float", Parameters: map[string]any{
				"generator": "deterministic", "min_value": 0.0, "max_value": 1000.0,
			}},
		},
	}}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(t),
		Processor: testPostgresProcessorCfg(withTransformerRules(rules)),
	}
	runStream(t, ctx, cfg)

	// whole numbers on purpose: wal2json emits those unquoted, which is the
	// shape that used to be rejected. Row 2 mixes in fractional values.
	execQuery(t, ctx, fmt.Sprintf(
		`INSERT INTO %s(id, amount, lat, dbl) VALUES
			(1, 1234, 51, 1000),
			(2, 1234.5678, 51.507351, 1000.5)`, testTable))

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	type row struct {
		id     int
		amount *float64
		lat    *float64
		dbl    *float64
	}
	query := fmt.Sprintf("SELECT id, amount, lat, dbl FROM %s ORDER BY id", testTable)

	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var got []row
	for got == nil {
		select {
		case <-timer.C:
			t.Fatal("timeout waiting for replicated numeric columns")
		case <-ticker.C:
			rows, err := targetConn.Query(ctx, query)
			if err != nil {
				continue
			}
			out := []row{}
			for rows.Next() {
				var r row
				if err := rows.Scan(&r.id, &r.amount, &r.lat, &r.dbl); err != nil {
					break
				}
				out = append(out, r)
			}
			rows.Close()
			if len(out) == 2 {
				got = out
			}
		}
	}

	for _, r := range got {
		// a rejected value is nulled by the default on_error policy, so a nil
		// here is the exact regression this test exists for
		require.NotNil(t, r.amount, "row %d: amount was nulled, the transformer rejected the replicated value", r.id)
		require.NotNil(t, r.lat, "row %d: lat was nulled, the transformer rejected the replicated value", r.id)
		require.NotNil(t, r.dbl, "row %d: dbl was nulled, the transformer rejected the replicated value", r.id)

		require.GreaterOrEqual(t, *r.amount, 0.0)
		require.LessOrEqual(t, *r.amount, 1000.0)
		require.GreaterOrEqual(t, *r.lat, -90.0)
		require.LessOrEqual(t, *r.lat, 90.0)
		require.GreaterOrEqual(t, *r.dbl, 0.0)
		require.LessOrEqual(t, *r.dbl, 1000.0)
	}
	// the source values must not survive untransformed
	require.NotEqual(t, 1234.0, *got[0].amount)
	require.NotEqual(t, 51.0, *got[0].lat)
}
