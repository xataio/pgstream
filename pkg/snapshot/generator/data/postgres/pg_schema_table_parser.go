// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"slices"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/snapshot"
)

type schemaTableParser struct {
	conn pglib.Querier
}

func newSchemaTableParser(conn pglib.Querier) *schemaTableParser {
	return &schemaTableParser{
		conn: conn,
	}
}

func (p *schemaTableParser) parseSnapshotTables(ctx context.Context, snapshot *snapshot.Snapshot) error {
	if slices.Contains(snapshot.TableNames, "*") {
		var err error
		snapshot.TableNames, err = p.discoverAllSchemaTables(ctx, snapshot.SchemaName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *schemaTableParser) discoverAllSchemaTables(ctx context.Context, schema string) ([]string, error) {
	const query = "SELECT tablename FROM pg_tables WHERE schemaname=$1"
	rows, err := p.conn.Query(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("discovering all tables for schema %s: %w", schema, err)
	}
	defer rows.Close()

	tableNames := []string{}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("scanning table name: %w", err)
		}
		tableNames = append(tableNames, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tableNames, nil
}
