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
		Listener:  testPostgresListenerCfg(),
		Processor: testPostgresProcessorCfgWithTransformer(pgurl),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runStream(t, ctx, cfg)

	testTable := "pg2pg_integration_transformer_test"
	execQuery(t, ctx, fmt.Sprintf(createTableQuery, testTable))

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
					if validateRows(t, ctx, targetConn, insertedRows, testTable) {
						return
					}
				}
			}
		})
	}
}

func insertQuery(table string, rows []transformerTestTableRow) string {
	query := fmt.Sprintf("INSERT INTO %s (name, last_name, email, address, age, total_purchases, customer_id, birth_date, is_active, created_at, updated_at, gender) VALUES", table)
	for i, row := range rows {
		if i > 0 {
			query += ","
		}
		query += fmt.Sprintf(" ('%s', '%s', '%s', '%s', %d, %f, '%s', '%s', %t, %d, '%s', '%s')",
			row.name, row.lastName, row.email, row.address, row.age, row.totalPurchases, row.customerID, row.birthDate.Format(time.DateOnly), row.isActive, row.createdAt, row.updatedAt.Format(time.RFC3339), row.gender)
	}
	return query
}

func validateRows(t *testing.T, ctx context.Context, conn *pglib.Conn, expectedRows []int, table string) bool {
	selectQuery := fmt.Sprintf("SELECT id, name, last_name, email, address, age, total_purchases, customer_id, birth_date, is_active, created_at, updated_at, gender FROM %s WHERE id IN (", table)
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
		err := rows.Scan(&row.id, &row.name, &row.lastName, &row.email, &row.address, &row.age, &row.totalPurchases, &row.customerID, &row.birthDate, &row.isActive, &row.createdAt, &row.updatedAt, &row.gender)
		require.NoError(t, err)
		rowsFromDB = append(rowsFromDB, row)
	}
	require.NoError(t, rows.Err())

	if len(rowsFromDB) != len(expectedRows) {
		return false
	}

	for _, row := range rowsFromDB {
		require.LessOrEqual(t, len(row.name), 5)
		require.LessOrEqual(t, len(row.lastName), 10)
		require.LessOrEqual(t, len(row.email), 15)
		require.LessOrEqual(t, len(row.address), 20)
		require.GreaterOrEqual(t, row.age, 18)
		require.LessOrEqual(t, row.age, 75)
		require.GreaterOrEqual(t, row.totalPurchases, 0.0)
		require.LessOrEqual(t, row.totalPurchases, 1000.0)
		require.NotEmpty(t, row.customerID)
		require.GreaterOrEqual(t, row.birthDate, time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC))
		require.LessOrEqual(t, row.birthDate, time.Date(2000, 12, 31, 23, 59, 59, 0, time.UTC))
		require.NotNil(t, row.isActive)
		require.GreaterOrEqual(t, row.createdAt, int64(1741856058))
		require.LessOrEqual(t, row.createdAt, int64(1741956058))
		require.GreaterOrEqual(t, row.updatedAt, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC))
		require.LessOrEqual(t, row.updatedAt, time.Date(2024, 1, 1, 23, 59, 59, 0, time.UTC))
		require.True(t, row.gender == "M" || row.gender == "F" || row.gender == "None")
	}
	return true
}
