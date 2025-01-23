// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/internal/log/zerolog"
	"github.com/xataio/pgstream/pkg/kafka"
	kafkamocks "github.com/xataio/pgstream/pkg/kafka/mocks"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
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

	testCommitPosition := wal.CommitPosition(testLSNStr)

	testBytes := []byte("test")
	mockMarshaler := func(any) ([]byte, error) { return testBytes, nil }

	tests := []struct {
		name            string
		walEvent        *wal.Event
		eventSerialiser func(any) ([]byte, error)
		batchSender     *batchmocks.BatchSender[kafka.Message]

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
			name: "ok - pgstream schema event",
			walEvent: &wal.Event{
				Data: &wal.Data{
					Action: "I",
					LSN:    testLSNStr,
					Schema: schemalog.SchemaName,
					Table:  schemalog.TableName,
					Columns: []wal.Column{
						{Name: "schema_name", Value: testSchema},
					},
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
			name:            "error - marshaling event",
			walEvent:        testWalEvent,
			eventSerialiser: func(any) ([]byte, error) { return nil, errTest },
			batchSender:     batchmocks.NewBatchSender[kafka.Message](),

			wantMsgs: []*batch.WALMessage[kafka.Message]{},
			wantErr:  errTest,
		},
		{
			name: "panic recovery - invalid schema value type",
			walEvent: &wal.Event{
				Data: &wal.Data{
					Action: "I",
					LSN:    testLSNStr,
					Schema: schemalog.SchemaName,
					Table:  schemalog.TableName,
					Columns: []wal.Column{
						{Name: "schema_name", Value: 1},
					},
				},
				CommitPosition: testCommitPosition,
			},
			batchSender: batchmocks.NewBatchSender[kafka.Message](),

			wantMsgs: []*batch.WALMessage[kafka.Message]{},
			wantErr:  errors.New("kafka batch writer: understanding event: schema_log schema_name received is not a string: int"),
		},
		{
			name: "panic recovery - schema_name not found",
			walEvent: &wal.Event{
				Data: &wal.Data{
					Action: "I",
					LSN:    testLSNStr,
					Schema: schemalog.SchemaName,
					Table:  schemalog.TableName,
				},
				CommitPosition: testCommitPosition,
			},
			batchSender: batchmocks.NewBatchSender[kafka.Message](),

			wantMsgs: []*batch.WALMessage[kafka.Message]{},
			wantErr:  errors.New("kafka batch writer: understanding event: schema_log schema_name not found in columns"),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			writer := &BatchWriter{
				logger:        loglib.NewNoopLogger(),
				maxBatchBytes: 100,
				serialiser:    mockMarshaler,
				batchSender:   tc.batchSender,
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
		tc := tc
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

func TestBatchKafkaWriter(t *testing.T) {
	t.Parallel()

	bw := BatchWriter{
		logger: newTestLogger(),
		writer: &kafkamocks.Writer{
			WriteMessagesFn: func(ctx context.Context, i uint64, msgs ...kafka.Message) error {
				time.Sleep(time.Second)
				return errTest
			},
		},
		maxBatchBytes: 10000,
		serialiser:    json.Marshal,
	}
	defer bw.Close()

	var err error
	bw.batchSender, err = batch.NewSender(&batch.Config{
		BatchTimeout:  time.Second,
		MaxBatchSize:  10,
		MaxBatchBytes: 10000,
	}, bw.sendBatch, bw.logger)
	require.NoError(t, err)

	doneChan := make(chan struct{}, 1)
	go func() {
		err := bw.batchSender.Send(context.Background())
		require.ErrorIs(t, err, errTest)
		doneChan <- struct{}{}
		close(doneChan)
	}()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	var processErr error
	for {
		select {
		case <-doneChan:
			require.ErrorIs(t, processErr, errTest)
			return
		case <-timer.C:
			t.Error("test timeout")
			return
		default:
			processErr = bw.ProcessWALEvent(context.Background(), &wal.Event{
				CommitPosition: wal.CommitPosition("1"),
				Data: &wal.Data{
					Action: "I",
				},
			})
		}
	}
}

func newTestLogger() loglib.Logger {
	return zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "trace",
	}))
}
