// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
	batchmocks "github.com/xataio/pgstream/pkg/wal/processor/batch/mocks"
)

// Queue DDL and subsequent DML before sending either. This catches catalog
// lookups made before the target has applied the DDL, as well as lost sequence
// mappings and identity values incorrectly filtered from INSERTs.
func TestBatchWriter_identitySequencesAfterDDL(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}
	ctx := context.Background()
	for _, image := range []testcontainers.PostgresImage{testcontainers.Postgres14, testcontainers.Postgres17} {
		t.Run(string(image), func(t *testing.T) {
			var pgURL string
			cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, image)
			require.NoError(t, err)
			defer cleanup()
			conn, err := pglib.NewConn(ctx, pgURL)
			require.NoError(t, err)
			defer conn.Close(ctx)

			for i, identity := range []string{"ALWAYS", "BY DEFAULT"} {
				t.Run(identity, func(t *testing.T) {
					schema := fmt.Sprintf("identity_%d", i)
					table := schema + ".events"
					sequence := schema + ".custom_counter"
					exec := func(sql string) {
						_, err := conn.Exec(ctx, sql)
						require.NoError(t, err, sql)
					}
					exec("CREATE SCHEMA " + schema)
					exec(fmt.Sprintf("CREATE TABLE %s (id bigint GENERATED %s AS IDENTITY PRIMARY KEY, payload text)", table, identity))
					// Sequence names cannot safely be guessed from table/column names.
					exec("ALTER SEQUENCE " + table + "_id_seq RENAME TO custom_counter")
					cfg := &Config{URL: pgURL, OnConflictAction: "error"}
					cfg.RetryPolicy.DisableRetries = true
					adapter, err := newAdapter(ctx, loglib.NewNoopLogger(), cfg, false, 4)
					require.NoError(t, err)
					defer adapter.close()
					var queued []*walMessage
					sender := batchmocks.NewBatchSender[*walMessage]()
					defer sender.Close()
					sender.SendMessageFn = func(_ context.Context, msg *batch.WALMessage[*walMessage]) error {
						queued = append(queued, msg.GetMessage())
						return nil
					}
					writer := &BatchWriter{
						Writer:     &Writer{logger: loglib.NewNoopLogger(), pgConn: conn, adapter: adapter, strictMode: true},
						dmlAdapter: mustNewDMLAdapter(t), batchSender: sender,
					}
					queue := func(data *wal.Data) {
						require.NoError(t, writer.ProcessWALEvent(ctx, &wal.Event{Data: data}))
					}
					flush := func() {
						require.NoError(t, writer.sendBatch(ctx, batch.NewBatch(queued, nil)))
						queued = nil
					}
					columns := func(idName string, isIdentity bool) []wal.DDLColumn {
						id := wal.DDLColumn{Name: idName, Type: "bigint", Attnum: 1}
						if isIdentity {
							id.Identity = &identity
						}
						return []wal.DDLColumn{id, {Name: "payload", Type: "text", Attnum: 2}, {Name: "note", Type: "text", Attnum: 3}}
					}
					ddl := func(sql, objectType, objectName string, cols []wal.DDLColumn) {
						commandTag := "ALTER TABLE"
						if strings.HasPrefix(sql, "CREATE TABLE") {
							commandTag = "CREATE TABLE"
						}
						content, err := json.Marshal(wal.DDLEvent{DDL: sql, SchemaName: schema, CommandTag: commandTag, Objects: []wal.DDLObject{
							{Type: objectType, Identity: objectName, Schema: schema, Columns: cols},
						}})
						require.NoError(t, err)
						queue(&wal.Data{Action: wal.LogicalMessageAction, Prefix: wal.DDLPrefix, Content: string(content)})
					}
					insert := func(tableName, idName string, id int64) {
						queue(&wal.Data{Action: "I", Schema: schema, Table: tableName, Columns: []wal.Column{
							{Name: idName, Type: "bigint", Value: id}, {Name: "payload", Type: "text", Value: fmt.Sprint(id)},
						}})
					}
					check := func(tableName, idName, seq string, id int) {
						requireCount(t, conn, fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE %s = %d AND payload = '%d'", schema, tableName, idName, id, id), 1)
						requireCount(t, conn, "SELECT last_value FROM "+seq, id)
					}

					insert("events", "id", 1001)
					flush()
					check("events", "id", sequence, 1001)

					ddl("ALTER TABLE "+table+" ADD COLUMN note text", "table", table, columns("id", true))
					insert("events", "id", 2001)
					// Neither SQL execution nor metadata resolution should occur at intake.
					requireCount(t, conn, fmt.Sprintf("SELECT count(*) FROM information_schema.columns WHERE table_schema='%s' AND table_name='events' AND column_name='note'", schema), 0)
					flush()
					check("events", "id", sequence, 2001)

					// Column-shaped DDL must invalidate the table key, and discovery
					// must use the renamed column after the target ALTER completes.
					ddl("ALTER TABLE "+table+" RENAME COLUMN id TO renamed_id", "table column", table+".renamed_id", columns("renamed_id", true))
					insert("events", "renamed_id", 3001)
					flush()
					check("events", "renamed_id", sequence, 3001)

					// Dropping identity must not leave a cached reference to its
					// now-removed sequence.
					ddl("ALTER TABLE "+table+" ALTER COLUMN renamed_id DROP IDENTITY", "table", table, columns("renamed_id", false))
					insert("events", "renamed_id", 4001)
					flush()
					requireCount(t, conn, "SELECT count(*) FROM "+table+" WHERE renamed_id=4001", 1)
					requireCount(t, conn, "SELECT count(*) FROM pg_class WHERE oid=to_regclass('"+sequence+"')", 0)

					// Cold CREATE followed by INSERT in the same batch cannot query
					// an absent target table and cache an empty sequence mapping.
					cold := schema + ".cold"
					ddl(fmt.Sprintf("CREATE TABLE %s (id bigint GENERATED %s AS IDENTITY PRIMARY KEY, payload text, note text)", cold, identity), "table", cold, columns("id", true))
					insert("cold", "id", 5001)
					flush()
					check("cold", "id", cold+"_id_seq", 5001)
				})
			}
		})
	}
}
