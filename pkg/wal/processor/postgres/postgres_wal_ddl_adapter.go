// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal"
)

type ddlAdapter struct{}

func newDDLAdapter() *ddlAdapter {
	return &ddlAdapter{}
}

func (a *ddlAdapter) walDataToQueries(ctx context.Context, d *wal.Data) ([]*query, error) {
	ddlEvent, err := wal.WalDataToDDLEvent(d)
	if err != nil {
		return nil, err
	}

	tableName := ""
	tableObjects := ddlEvent.GetTableObjects()
	if len(tableObjects) > 0 {
		tableName = tableObjects[0].GetTable()
	}

	return []*query{
		a.newDDLQuery(ddlEvent.SchemaName, tableName, ddlEvent.DDL),
	}, nil
}

func (a *ddlAdapter) newDDLQuery(schema, table, sql string) *query {
	return &query{
		schema: schema,
		table:  table,
		sql:    sql,
		isDDL:  true,
	}
}
