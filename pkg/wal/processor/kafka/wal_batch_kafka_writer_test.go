// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/kafka"
	kafkamocks "github.com/xataio/pgstream/internal/kafka/mocks"
	"github.com/xataio/pgstream/internal/replication"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"

	"golang.org/x/sync/semaphore"
)

var (
	testSchema = "test_schema"
	testTable  = "test_table"

	testLSN    = replication.LSN(7773397064)
	testLSNStr = "1/CF54A048"

	errTest = errors.New("oh noes")
)

func TestBatchKafkaWriter_ProcessWALEvent(t *testing.T) {
	t.Parallel()

	testWalEvent := &wal.Data{
		Action: "I",
		LSN:    testLSNStr,
		Schema: testSchema,
		Table:  testTable,
	}

	testBytes := []byte("test")
	mockMarshaler := func(any) ([]byte, error) { return testBytes, nil }

	tests := []struct {
		name            string
		walEvent        *wal.Data
		pos             commitPosition
		eventSerialiser func(any) ([]byte, error)
		semaphore       weightedSemaphore

		wantMsgs []*kafkaMsg
		wantErr  error
	}{
		{
			name:     "ok",
			walEvent: testWalEvent,
			pos:      commitPosition{pgPos: testLSN},

			wantMsgs: []*kafkaMsg{
				{
					msg: kafka.Message{
						Key:   []byte(testSchema),
						Value: testBytes,
					},
					pos: testLSN,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - pgstream schema event",
			walEvent: &wal.Data{
				Action: "I",
				LSN:    testLSNStr,
				Schema: schemalog.SchemaName,
				Table:  schemalog.TableName,
				Columns: []wal.Column{
					{Name: "schema_name", Value: testSchema},
				},
			},
			pos: commitPosition{pgPos: testLSN},

			wantMsgs: []*kafkaMsg{
				{
					msg: kafka.Message{
						Key:   []byte(testSchema),
						Value: testBytes,
					},
					pos: testLSN,
				},
			},
			wantErr: nil,
		},
		{
			name:            "ok - wal event too large, message dropped",
			walEvent:        testWalEvent,
			pos:             commitPosition{pgPos: testLSN},
			eventSerialiser: func(any) ([]byte, error) { return []byte(strings.Repeat("a", 101)), nil },

			wantMsgs: []*kafkaMsg{},
			wantErr:  nil,
		},
		{
			name:            "error - marshaling event",
			walEvent:        testWalEvent,
			pos:             commitPosition{pgPos: testLSN},
			eventSerialiser: func(any) ([]byte, error) { return nil, errTest },

			wantMsgs: []*kafkaMsg{},
			wantErr:  errTest,
		},
		{
			name: "panic recovery - invalid schema value type",
			walEvent: &wal.Data{
				Action: "I",
				LSN:    testLSNStr,
				Schema: schemalog.SchemaName,
				Table:  schemalog.TableName,
				Columns: []wal.Column{
					{Name: "schema_name", Value: 1},
				},
			},
			pos: commitPosition{pgPos: testLSN},

			wantMsgs: []*kafkaMsg{},
			wantErr:  errors.New("kafka batch writer: understanding event: schema_log schema_name received is not a string: int"),
		},
		{
			name: "panic recovery - schema_name not found",
			walEvent: &wal.Data{
				Action: "I",
				LSN:    testLSNStr,
				Schema: schemalog.SchemaName,
				Table:  schemalog.TableName,
			},
			pos: commitPosition{pgPos: testLSN},

			wantMsgs: []*kafkaMsg{},
			wantErr:  errors.New("kafka batch writer: understanding event: schema_log schema_name not found in columns"),
		},
		{
			name:     "error - acquiring semaphore",
			walEvent: testWalEvent,
			pos:      commitPosition{pgPos: testLSN},
			semaphore: &mockWeightedSemaphore{
				tryAcquireFn: func(int64) bool { return false },
				acquireFn:    func(_ context.Context, i int64) error { return errTest },
			},

			wantMsgs: []*kafkaMsg{},
			wantErr:  errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			writer := &BatchKafkaWriter{
				msgChan:         make(chan *kafkaMsg),
				maxBatchBytes:   100,
				queueBytesSema:  semaphore.NewWeighted(defaultMaxQueueBytes),
				eventSerialiser: mockMarshaler,
			}

			if tc.semaphore != nil {
				writer.queueBytesSema = tc.semaphore
			}

			if tc.eventSerialiser != nil {
				writer.eventSerialiser = tc.eventSerialiser
			}

			go func() {
				defer close(writer.msgChan)
				err := writer.ProcessWALEvent(context.Background(), tc.walEvent, tc.pos)
				if !errors.Is(err, tc.wantErr) {
					require.Equal(t, err.Error(), tc.wantErr.Error())
				}
			}()

			msgs := []*kafkaMsg{}
			for msg := range writer.msgChan {
				msgs = append(msgs, msg)
				writer.queueBytesSema.Release(int64(msg.size()))
			}
			require.Equal(t, tc.wantMsgs, msgs)
		})
	}
}

func TestBatchKafkaWriter_SendThread(t *testing.T) {
	t.Parallel()

	testBytes := []byte("test")
	testKafkaMsg := &kafkaMsg{
		msg: kafka.Message{
			Key:   []byte(testSchema),
			Value: testBytes,
		},
		pos: testLSN,
	}

	tests := []struct {
		name             string
		writerValidation func(i uint64, doneChan chan struct{}, msgs ...kafka.Message) error
		msgs             []*kafkaMsg
		semaphore        *mockWeightedSemaphore

		wantWriteCalls   uint64
		wantReleaseCalls uint64
		wantErr          error
	}{
		{
			name: "ok",
			msgs: []*kafkaMsg{testKafkaMsg},
			writerValidation: func(i uint64, doneChan chan struct{}, msgs ...kafka.Message) error {
				defer func() {
					doneChan <- struct{}{}
				}()
				if i == 1 {
					require.Equal(t, 1, len(msgs))
					require.Equal(t, testBytes, msgs[0].Value)
					require.Equal(t, testSchema, string(msgs[0].Key))
					return nil
				}
				return fmt.Errorf("unexpected write call: %d", i)
			},
			semaphore: &mockWeightedSemaphore{
				releaseFn: func(_ uint64, size int64) {
					require.Equal(t, len(testBytes), int(size))
				},
			},

			wantWriteCalls:   1,
			wantReleaseCalls: 1,
			wantErr:          context.Canceled,
		},
		{
			name: "ok - max batch bytes reached, trigger send",
			msgs: []*kafkaMsg{
				{
					msg: kafka.Message{
						Key:   []byte(testSchema),
						Value: []byte(strings.Repeat("a", 51)),
					},
					pos: testLSN,
				},
				{
					msg: kafka.Message{
						Key:   []byte(testSchema),
						Value: []byte(strings.Repeat("b", 50)),
					},
					pos: testLSN,
				},
				{
					msg: kafka.Message{
						Key:   []byte(testSchema),
						Value: []byte(strings.Repeat("c", 10)),
					},
					pos: testLSN,
				},
			},
			writerValidation: func(i uint64, doneChan chan struct{}, msgs ...kafka.Message) error {
				defer func() {
					if i == 2 {
						doneChan <- struct{}{}
					}
				}()
				switch i {
				case 1:
					require.Equal(t, 1, len(msgs))
					require.Equal(t, []byte(strings.Repeat("a", 51)), msgs[0].Value)
					require.Equal(t, testSchema, string(msgs[0].Key))
					return nil
				case 2:
					require.Equal(t, 2, len(msgs))
					require.Equal(t, []byte(strings.Repeat("b", 50)), msgs[0].Value)
					require.Equal(t, testSchema, string(msgs[0].Key))
					require.Equal(t, []byte(strings.Repeat("c", 10)), msgs[1].Value)
					require.Equal(t, testSchema, string(msgs[1].Key))
				}
				return nil
			},
			semaphore: &mockWeightedSemaphore{
				releaseFn: func(i uint64, size int64) {
					switch i {
					case 1:
						require.Equal(t, int64(51), size)
					case 2:
						require.Equal(t, int64(60), size)
					default:
						require.Fail(t, fmt.Sprintf("unexpected call to release: %d", i))
					}
				},
			},

			wantWriteCalls:   2,
			wantReleaseCalls: 2,
			wantErr:          context.Canceled,
		},
		{
			name: "error - writing messages",
			msgs: []*kafkaMsg{testKafkaMsg},
			writerValidation: func(i uint64, doneChan chan struct{}, msgs ...kafka.Message) error {
				defer func() {
					doneChan <- struct{}{}
				}()
				return errTest
			},
			semaphore: &mockWeightedSemaphore{
				releaseFn: func(_ uint64, size int64) {
					require.Equal(t, len(testBytes), int(size))
				},
			},

			wantWriteCalls:   1,
			wantReleaseCalls: 1,
			wantErr:          errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doneChan := make(chan struct{})
			mockWriter := &kafkamocks.Writer{
				WriteMessagesFn: func(ctx context.Context, i uint64, msgs ...kafka.Message) error {
					return tc.writerValidation(i, doneChan, msgs...)
				},
			}

			writer := &BatchKafkaWriter{
				writer:         mockWriter,
				msgChan:        make(chan *kafkaMsg),
				maxBatchBytes:  100,
				maxBatchSize:   10,
				queueBytesSema: tc.semaphore,
				sendFrequency:  time.Second,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := writer.SendThread(ctx)
				require.ErrorIs(t, err, tc.wantErr)
				require.Equal(t, tc.wantWriteCalls, mockWriter.GetWriteCalls())
				require.Equal(t, tc.wantReleaseCalls, tc.semaphore.getReleaseCalls())
			}()

			for _, msg := range tc.msgs {
				writer.msgChan <- msg
			}
			// make sure the test doesn't block indefinitely if something goes
			// wrong
			for {
				select {
				case <-doneChan:
					if errors.Is(tc.wantErr, context.Canceled) {
						cancel()
					}
					wg.Wait()
					return
				case <-ctx.Done():
					wg.Wait()
					return
				}
			}
		})
	}
}

func TestBatchKafkaWriter_sendBatch(t *testing.T) {
	t.Parallel()

	testBytes := []byte("test")
	testBatch := &kafkaMsgBatch{
		msgs: []kafka.Message{
			{
				Key:   []byte(testSchema),
				Value: testBytes,
			},
		},
		lastPos: testLSN,
	}

	tests := []struct {
		name       string
		writer     *kafkamocks.Writer
		checkpoint checkpoint
		batch      *kafkaMsgBatch

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
			checkpoint: func(_ context.Context, commitPos []commitPosition) error {
				require.Equal(t, 1, len(commitPos))
				require.Equal(t, testLSN, commitPos[0].pgPos)
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
			checkpoint: func(_ context.Context, commitPos []commitPosition) error {
				return errors.New("checkpoint: should not be called")
			},
			batch: &kafkaMsgBatch{
				msgs: []kafka.Message{},
			},

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
			checkpoint: func(_ context.Context, commitPos []commitPosition) error {
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
			checkpoint: func(_ context.Context, commitPos []commitPosition) error {
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

			writer := &BatchKafkaWriter{
				writer:       tc.writer,
				checkpointer: tc.checkpoint,
			}

			err := writer.sendBatch(context.Background(), tc.batch)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

type mockWeightedSemaphore struct {
	tryAcquireFn func(int64) bool
	acquireFn    func(context.Context, int64) error
	releaseFn    func(uint64, int64)
	releaseCalls uint64
}

func (m *mockWeightedSemaphore) TryAcquire(i int64) bool {
	return m.tryAcquireFn(i)
}

func (m *mockWeightedSemaphore) Acquire(ctx context.Context, i int64) error {
	return m.acquireFn(ctx, i)
}

func (m *mockWeightedSemaphore) Release(i int64) {
	atomic.AddUint64(&m.releaseCalls, 1)
	m.releaseFn(m.getReleaseCalls(), i)
}

func (m *mockWeightedSemaphore) getReleaseCalls() uint64 {
	return atomic.LoadUint64(&m.releaseCalls)
}
