// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/pkg/kafka"
	kafkamocks "github.com/xataio/pgstream/pkg/kafka/mocks"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
	batchmocks "github.com/xataio/pgstream/pkg/wal/processor/batch/mocks"
)

var (
	testSchema = "test_schema"
	testTable  = "test_table"

	testLSNStr = "1/CF54A048"

	errTest = errors.New("oh noes")
)

func TestBatchKafkaWriter_ProcessWALEvent(t *testing.T) {
	t.Parallel()

	testWalEvent := &wal.Event{
		Data: &wal.Data{
			Action: "I",
			LSN:    testLSNStr,
			Schema: testSchema,
			Table:  testTable,
		},
		CommitPosition: wal.CommitPosition(testLSNStr),
	}

	testDDLEvent := &wal.DDLEvent{
		DDL:        "CREATE TABLE test_schema.test_table (col-1 text PRIMARY KEY, col-2 integer);",
		SchemaName: testSchema,
		CommandTag: "CREATE TABLE",
		Objects: []wal.DDLObject{
			{
				Type:     "table",
				Identity: "test_schema.test_table",
				Schema:   "test_schema",
				OID:      "123456",
				Columns: []wal.DDLColumn{
					{Attnum: 1, Name: "col-1", Type: "text", Nullable: false, Generated: false, Unique: true},
					{Attnum: 2, Name: "col-2", Type: "integer", Nullable: true, Generated: false, Unique: false},
				},
				PrimaryKeyColumns: []string{"col-1"},
			},
		},
	}

	ddlContentBytes, err := json.Marshal(testDDLEvent)
	require.NoError(t, err)

	testCommitPosition := wal.CommitPosition(testLSNStr)

	testBytes := []byte("test")
	mockMarshaler := func(any) ([]byte, error) { return testBytes, nil }

	tests := []struct {
		name              string
		walEvent          *wal.Event
		eventSerialiser   func(any) ([]byte, error)
		batchSender       *batchmocks.BatchSender[kafka.Message]
		walDataToDDLEvent func(*wal.Data) (*wal.DDLEvent, error)

		wantMsgs []*batch.WALMessage[kafka.Message]
		wantErr  error
	}{
		{
			name:        "ok",
			walEvent:    testWalEvent,
			batchSender: batchmocks.NewBatchSender[kafka.Message](),

			wantMsgs: []*batch.WALMessage[kafka.Message]{
				batch.NewWALMessage(kafka.Message{
					Key:   []byte(testSchema),
					Value: testBytes,
				}, testCommitPosition),
			},
			wantErr: nil,
		},
		{
			name: "ok - keep alive",
			walEvent: &wal.Event{
				CommitPosition: testCommitPosition,
			},
			batchSender: batchmocks.NewBatchSender[kafka.Message](),

			wantMsgs: []*batch.WALMessage[kafka.Message]{
				batch.NewWALMessage(kafka.Message{}, testCommitPosition),
			},
			wantErr: nil,
		},
		{
			name: "ok - pgstream DDL event",
			walEvent: &wal.Event{
				Data: &wal.Data{
					Action:  wal.LogicalMessageAction,
					Prefix:  wal.DDLPrefix,
					LSN:     testLSNStr,
					Content: string(ddlContentBytes),
				},
				CommitPosition: testCommitPosition,
			},
			batchSender: batchmocks.NewBatchSender[kafka.Message](),

			wantMsgs: []*batch.WALMessage[kafka.Message]{
				batch.NewWALMessage(kafka.Message{
					Key:   []byte(testSchema),
					Value: testBytes,
				}, testCommitPosition),
			},
			wantErr: nil,
		},
		{
			name:            "ok - wal event too large, message dropped",
			walEvent:        testWalEvent,
			eventSerialiser: func(any) ([]byte, error) { return []byte(strings.Repeat("a", 101)), nil },
			batchSender:     batchmocks.NewBatchSender[kafka.Message](),

			wantMsgs: []*batch.WALMessage[kafka.Message]{},
			wantErr:  nil,
		},
		{
			// the key carries primary key values and counts towards the record
			// size the broker enforces: 90 bytes of value fits under the 95%
			// threshold on its own, but not once the 11 byte key is added
			name:            "ok - wal event too large once the key is counted, message dropped",
			walEvent:        testWalEvent,
			eventSerialiser: func(any) ([]byte, error) { return []byte(strings.Repeat("a", 90)), nil },
			batchSender:     batchmocks.NewBatchSender[kafka.Message](),

			wantMsgs: []*batch.WALMessage[kafka.Message]{},
			wantErr:  nil,
		},
		{
			name:            "error - marshaling event",
			walEvent:        testWalEvent,
			eventSerialiser: func(any) ([]byte, error) { return nil, errTest },
			batchSender:     batchmocks.NewBatchSender[kafka.Message](),

			wantMsgs: []*batch.WALMessage[kafka.Message]{},
			wantErr:  errTest,
		},
		{
			name: "error - parsing DDL event",
			walEvent: &wal.Event{
				Data: &wal.Data{
					Action:  wal.LogicalMessageAction,
					Prefix:  wal.DDLPrefix,
					LSN:     testLSNStr,
					Content: string(ddlContentBytes),
				},
				CommitPosition: testCommitPosition,
			},
			batchSender: batchmocks.NewBatchSender[kafka.Message](),
			walDataToDDLEvent: func(*wal.Data) (*wal.DDLEvent, error) {
				return nil, errTest
			},

			wantMsgs: []*batch.WALMessage[kafka.Message]{
				batch.NewWALMessage(kafka.Message{
					Key:   []byte(""),
					Value: testBytes,
				}, testCommitPosition),
			},
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			writer := &BatchWriter{
				logger:            loglib.NewNoopLogger(),
				maxBatchBytes:     100,
				serialiser:        mockMarshaler,
				batchSender:       tc.batchSender,
				walDataToDDLEvent: wal.WalDataToDDLEvent,
			}

			if tc.walDataToDDLEvent != nil {
				writer.walDataToDDLEvent = tc.walDataToDDLEvent
			}

			if tc.eventSerialiser != nil {
				writer.serialiser = tc.eventSerialiser
			}

			go func() {
				defer tc.batchSender.Close()
				err := writer.ProcessWALEvent(context.Background(), tc.walEvent)
				if !errors.Is(err, tc.wantErr) {
					require.Equal(t, err.Error(), tc.wantErr.Error())
				}
			}()

			msgs := tc.batchSender.GetWALMessages()
			require.Equal(t, tc.wantMsgs, msgs)
		})
	}
}

func TestBatchKafkaWriter_getMessageKey(t *testing.T) {
	t.Parallel()

	testWalData := func() *wal.Data {
		return &wal.Data{
			Action: "I",
			LSN:    testLSNStr,
			Schema: testSchema,
			Table:  testTable,
			Columns: []wal.Column{
				{ID: "col-1", Name: "id", Type: "integer", Value: int64(1)},
				{ID: "col-2", Name: "name", Type: "text", Value: "alice"},
			},
			Metadata: wal.Metadata{
				InternalColIDs: []string{"col-1"},
			},
		}
	}

	testDDLData := &wal.Data{
		Action:  wal.LogicalMessageAction,
		Prefix:  wal.DDLPrefix,
		LSN:     testLSNStr,
		Content: `{"schema_name":"test_schema"}`,
	}

	tests := []struct {
		name         string
		partitionKey PartitionKey
		walData      *wal.Data

		wantKey string
	}{
		{
			name:         "schema key",
			partitionKey: PartitionKeySchema,
			walData:      testWalData(),

			wantKey: testSchema,
		},
		{
			name:         "default key",
			partitionKey: "",
			walData:      testWalData(),

			wantKey: testSchema,
		},
		{
			name:         "table key",
			partitionKey: PartitionKeyTable,
			walData:      testWalData(),

			wantKey: "test_schema.test_table",
		},
		{
			name:         "primary key",
			partitionKey: PartitionKeyPrimaryKey,
			walData:      testWalData(),

			wantKey: "test_schema.test_table:1",
		},
		{
			name:         "primary key - composite",
			partitionKey: PartitionKeyPrimaryKey,
			walData: func() *wal.Data {
				d := testWalData()
				d.Metadata.InternalColIDs = []string{"col-1", "col-2"}
				return d
			}(),

			wantKey: "test_schema.test_table:1,alice",
		},
		{
			// ["a,b","c"] and ["a","b,c"] both joined to "a,b,c" before the
			// delimiter was escaped, colliding on one message key
			name:         "primary key - composite with a comma in the first value",
			partitionKey: PartitionKeyPrimaryKey,
			walData: func() *wal.Data {
				d := testWalData()
				d.Columns = []wal.Column{
					{ID: "col-1", Name: "a", Type: "text", Value: "a,b"},
					{ID: "col-2", Name: "b", Type: "text", Value: "c"},
				}
				d.Metadata.InternalColIDs = []string{"col-1", "col-2"}
				return d
			}(),

			wantKey: `test_schema.test_table:a\,b,c`,
		},
		{
			name:         "primary key - composite with a comma in the second value",
			partitionKey: PartitionKeyPrimaryKey,
			walData: func() *wal.Data {
				d := testWalData()
				d.Columns = []wal.Column{
					{ID: "col-1", Name: "a", Type: "text", Value: "a"},
					{ID: "col-2", Name: "b", Type: "text", Value: "b,c"},
				}
				d.Metadata.InternalColIDs = []string{"col-1", "col-2"}
				return d
			}(),

			wantKey: `test_schema.test_table:a,b\,c`,
		},
		{
			// the escape character itself must be escaped, or ["a\","b"] and
			// ["a", ",b"] would collide in turn
			name:         "primary key - composite with a backslash in the value",
			partitionKey: PartitionKeyPrimaryKey,
			walData: func() *wal.Data {
				d := testWalData()
				d.Columns = []wal.Column{
					{ID: "col-1", Name: "a", Type: "text", Value: `a\`},
					{ID: "col-2", Name: "b", Type: "text", Value: "b"},
				}
				d.Metadata.InternalColIDs = []string{"col-1", "col-2"}
				return d
			}(),

			wantKey: `test_schema.test_table:a\\,b`,
		},
		{
			name:         "primary key - update event keys on the same row identity",
			partitionKey: PartitionKeyPrimaryKey,
			walData: func() *wal.Data {
				d := testWalData()
				d.Action = "U"
				d.Identity = d.Columns
				return d
			}(),

			wantKey: "test_schema.test_table:1",
		},
		{
			name:         "primary key - delete event with identity columns",
			partitionKey: PartitionKeyPrimaryKey,
			walData: func() *wal.Data {
				d := testWalData()
				d.Action = "D"
				d.Identity = d.Columns
				d.Columns = nil
				return d
			}(),

			wantKey: "test_schema.test_table:1",
		},
		{
			name:         "primary key - no metadata, degrades to table key",
			partitionKey: PartitionKeyPrimaryKey,
			walData: func() *wal.Data {
				d := testWalData()
				d.Metadata = wal.Metadata{}
				return d
			}(),

			wantKey: "test_schema.test_table",
		},
		{
			name:         "primary key - identity columns not in event, degrades to table key",
			partitionKey: PartitionKeyPrimaryKey,
			walData: func() *wal.Data {
				d := testWalData()
				d.Metadata.InternalColIDs = []string{"col-3"}
				return d
			}(),

			wantKey: "test_schema.test_table",
		},
		{
			name:         "ddl event keeps schema key regardless of strategy",
			partitionKey: PartitionKeyPrimaryKey,
			walData:      testDDLData,

			wantKey: testSchema,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			writer := BatchWriter{
				logger:            loglib.NewNoopLogger(),
				partitionKey:      tc.partitionKey,
				walDataToDDLEvent: wal.WalDataToDDLEvent,
			}

			key := writer.getMessageKey(tc.walData)
			require.Equal(t, tc.wantKey, string(key))
		})
	}
}

// TestPrimaryKeyMessageKey_DistinctTuples anchors the specific tuples that used
// to collide. The exhaustive version of this property lives in
// TestPrimaryKeyMessageKey_Injective; these are the regression cases worth
// naming, each paired with the tuple it used to be confused with.
func TestPrimaryKeyMessageKey_DistinctTuples(t *testing.T) {
	t.Parallel()

	tuples := [][]string{
		// collided on the comma delimiter
		{"a,b", "c"},
		{"a", "b,c"},
		{"a", "b", "c"},
		{"a,b,c"},
		// collide unless the escape character is itself escaped: `a\` escapes
		// to `a\\`, which must not read back as the single value "a,b"
		{`a\`, "b"},
		{"a,b"},
		{"a", `\b`},
		{`\`, ""},
		{`a\,b`, "c"},
		{"a", `,b,c`},
		// empty components, including the single empty value whose key sits one
		// character from the no-primary-key fallback key
		{""},
		{"", ","},
		{",", ""},
		{"", "", ""},
	}

	keys := make(map[string][]string, len(tuples))
	for _, tuple := range tuples {
		cols := make([]wal.Column, 0, len(tuple))
		colIDs := make([]string, 0, len(tuple))
		for i, v := range tuple {
			id := fmt.Sprintf("col-%d", i)
			cols = append(cols, wal.Column{ID: id, Name: id, Type: "text", Value: v})
			colIDs = append(colIDs, id)
		}

		key := string(primaryKeyMessageKey(&wal.Data{
			Schema:   testSchema,
			Table:    testTable,
			Columns:  cols,
			Metadata: wal.Metadata{InternalColIDs: colIDs},
		}))

		if clashing, found := keys[key]; found {
			t.Errorf("tuples %q and %q both encode to message key %q", clashing, tuple, key)
			continue
		}
		keys[key] = tuple
	}
}

// TestPrimaryKeyMessageKey_DistinctIdentifiers covers the other half of the
// encoding: postgres quoted identifiers may contain the "." and ":" that
// separate schema, table and value list, so those need escaping too.
func TestPrimaryKeyMessageKey_DistinctIdentifiers(t *testing.T) {
	t.Parallel()

	identities := []struct {
		schema string
		table  string
		value  string
	}{
		{schema: "a.b", table: "c", value: "1"},
		{schema: "a", table: "b.c", value: "1"},
		{schema: "s", table: "t:1", value: "x"},
		{schema: "s", table: "t", value: "1:x"},
		{schema: "s", table: `t\`, value: "x"},
	}

	keys := make(map[string]string, len(identities))
	for _, id := range identities {
		key := string(primaryKeyMessageKey(&wal.Data{
			Schema:   id.schema,
			Table:    id.table,
			Columns:  []wal.Column{{ID: "col-1", Name: "pk", Type: "text", Value: id.value}},
			Metadata: wal.Metadata{InternalColIDs: []string{"col-1"}},
		}))

		desc := fmt.Sprintf("schema=%q table=%q value=%q", id.schema, id.table, id.value)
		if clashing, found := keys[key]; found {
			t.Errorf("%s and %s both encode to message key %q", clashing, desc, key)
			continue
		}
		keys[key] = desc
	}
}

func TestConfig_partitionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		partitionKey PartitionKey

		wantKey PartitionKey
		wantErr bool
	}{
		{name: "default", partitionKey: "", wantKey: PartitionKeySchema},
		{name: "schema", partitionKey: PartitionKeySchema, wantKey: PartitionKeySchema},
		{name: "table", partitionKey: PartitionKeyTable, wantKey: PartitionKeyTable},
		{name: "primary key", partitionKey: PartitionKeyPrimaryKey, wantKey: PartitionKeyPrimaryKey},
		{name: "invalid", partitionKey: "invalid", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			config := &Config{PartitionKey: tc.partitionKey}
			key, err := config.partitionKey()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantKey, key)
		})
	}
}

func TestBatchKafkaWriter_sendBatch(t *testing.T) {
	t.Parallel()

	testCommitPosition := wal.CommitPosition(testLSNStr)
	testBytes := []byte("test")
	testBatch := batch.NewBatch(
		[]kafka.Message{
			{
				Key:   []byte(testSchema),
				Value: testBytes,
			},
		},
		[]wal.CommitPosition{testCommitPosition})

	tests := []struct {
		name       string
		writer     *kafkamocks.Writer
		checkpoint checkpointer.Checkpoint
		batch      *batch.Batch[kafka.Message]

		wantErr error
	}{
		{
			name: "ok",
			writer: &kafkamocks.Writer{
				WriteMessagesFn: func(ctx context.Context, i uint64, msgs ...kafka.Message) error {
					require.Equal(t, 1, len(msgs))
					require.Equal(t, testBytes, msgs[0].Value)
					require.Equal(t, testSchema, string(msgs[0].Key))
					return nil
				},
			},
			checkpoint: func(_ context.Context, commitPos []wal.CommitPosition) error {
				require.Equal(t, 1, len(commitPos))
				require.Equal(t, testCommitPosition, commitPos[0])
				return nil
			},
			batch: testBatch,

			wantErr: nil,
		},
		{
			name: "ok - empty batch",
			writer: &kafkamocks.Writer{
				WriteMessagesFn: func(ctx context.Context, i uint64, msgs ...kafka.Message) error {
					return errors.New("WriteMessagesFn: should not be called")
				},
			},
			checkpoint: func(_ context.Context, commitPos []wal.CommitPosition) error {
				return errors.New("checkpoint: should not be called")
			},
			batch: batch.NewBatch([]kafka.Message{}, nil),

			wantErr: nil,
		},
		{
			name: "ok - error checkpointing",
			writer: &kafkamocks.Writer{
				WriteMessagesFn: func(ctx context.Context, i uint64, msgs ...kafka.Message) error {
					require.Equal(t, 1, len(msgs))
					require.Equal(t, testBytes, msgs[0].Value)
					require.Equal(t, testSchema, string(msgs[0].Key))
					return nil
				},
			},
			checkpoint: func(_ context.Context, commitPos []wal.CommitPosition) error {
				return errTest
			},
			batch: testBatch,

			wantErr: nil,
		},
		{
			name: "error - writing messages",
			writer: &kafkamocks.Writer{
				WriteMessagesFn: func(ctx context.Context, i uint64, msgs ...kafka.Message) error {
					return errTest
				},
			},
			checkpoint: func(_ context.Context, commitPos []wal.CommitPosition) error {
				return errors.New("checkpoint: should not be called")
			},
			batch: testBatch,

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			writer := &BatchWriter{
				logger:       loglib.NewNoopLogger(),
				writer:       tc.writer,
				checkpointer: tc.checkpoint,
			}

			err := writer.sendBatch(context.Background(), tc.batch)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
