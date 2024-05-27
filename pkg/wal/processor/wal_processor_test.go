// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
)

func Test_WalDataToLogEntry(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Round(time.Second)
	nowStr := now.Format("2006-01-02 15:04:05")
	id := xid.New()

	testWalData := &wal.Data{
		Action: "I",
		Schema: schemalog.SchemaName,
		Table:  schemalog.TableName,
		Columns: []wal.Column{
			{ID: "id", Name: "id", Type: "text", Value: id.String()},
			{ID: "version", Name: "version", Type: "integer", Value: 0},
			{ID: "schema_name", Name: "schema_name", Type: "text", Value: "test_schema_1"},
			{ID: "created_at", Name: "created_at", Type: "timestamp", Value: nowStr},
		},
	}

	tests := []struct {
		name string
		data *wal.Data

		wantLogEntry *schemalog.LogEntry
		wantErr      error
	}{
		{
			name: "ok",
			data: testWalData,

			wantLogEntry: &schemalog.LogEntry{
				ID:         id,
				Version:    0,
				SchemaName: "test_schema_1",
				CreatedAt:  schemalog.NewSchemaCreatedAtTimestamp(now),
			},
			wantErr: nil,
		},
		{
			name: "error - invalid data",
			data: &wal.Data{
				Schema: "test_schema",
				Table:  "test_table",
			},

			wantLogEntry: nil,
			wantErr:      ErrIncompatibleWalData,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logEntry, err := WalDataToLogEntry(tc.data)
			require.ErrorIs(t, err, tc.wantErr)
			if diff := cmp.Diff(logEntry, tc.wantLogEntry, cmpopts.IgnoreUnexported(schemalog.LogEntry{})); diff != "" {
				t.Errorf("got: \n%v, \nwant \n%v, \ndiff: \n%s", logEntry, tc.wantLogEntry, diff)
			}
		})
	}
}
