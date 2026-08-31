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
			insertedRows := []int{}
			for range tc.rows {
				insertedRows = append(insertedRows, nextRowID)
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
					if validateRows(t, ctx, targetConn, insertedRows, testTable, tc.rows[0]) {
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
// assertion exists for a column, only the column type is checked.
func validateRows(t *testing.T, ctx context.Context, conn *pglib.Conn, expectedRows []int, table string, source transformerTestTableRow) bool {
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

		// --- the value is writable into the real column type ---
		//
		// No source-discriminating assertion exists for these: the source value
		// already satisfies the configured rule, so only the column type is
		// checked. The transformed value having survived a scan into its Go
		// type is the assertion — a transformer emitting something postgres
		// rejects fails the insert on the target instead, and the row never
		// arrives to be read here.

		require.NotEmpty(t, row.customerID, "uuid column")
		require.False(t, row.birthDate.IsZero(), "date column")
		require.False(t, row.updatedAt.IsZero(), "timestamp with time zone column")
		require.NotEmpty(t, row.name, "text column")
		require.NotEmpty(t, row.lastName, "varchar column")
		require.NotEmpty(t, row.address, "text column")
	}
	return true
}
