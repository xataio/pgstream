// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/replication"
)

func TestHandler_StartReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		replicationConn pglib.ReplicationQuerier
		connBuilder     func() (pglib.Querier, error)
		slotName        string

		wantErr error
	}{
		{
			name: "ok - with last synced LSN",
			replicationConn: &pgmocks.ReplicationConn{
				IdentifySystemFn: func(ctx context.Context) (pglib.IdentifySystemResult, error) {
					return pglib.IdentifySystemResult{
						DBName:   testDBName,
						SystemID: "tes-sys-id",
					}, nil
				},
				StartReplicationFn: func(ctx context.Context, cfg pglib.ReplicationConfig) error {
					require.Equal(t, testLSN, cfg.StartPos)
					require.Equal(t, testSlot, cfg.SlotName)
					require.Equal(t, pluginArguments, cfg.PluginArguments)
					return nil
				},
				SendStandbyStatusUpdateFn: func(ctx context.Context, lsn uint64) error {
					require.Equal(t, testLSN, lsn)
					return nil
				},
			},
			connBuilder: func() (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "select restart_lsn from pg_replication_slots where slot_name=$1":
							return errors.New("restart lsn should not be called")
						case "select confirmed_flush_lsn from pg_replication_slots where slot_name=$1":
							require.Len(t, dest, 1)
							lsn, ok := dest[0].(*string)
							require.True(t, ok)
							*lsn = testLSNStr
							return nil
						default:
							return fmt.Errorf("unexpected query: %s", query)
						}
					},
					CloseFn: func(ctx context.Context) error { return nil },
				}, nil
			},
			slotName: testSlot,

			wantErr: nil,
		},
		{
			name: "ok - with restart LSN",
			replicationConn: &pgmocks.ReplicationConn{
				IdentifySystemFn: func(ctx context.Context) (pglib.IdentifySystemResult, error) {
					return pglib.IdentifySystemResult{
						DBName:   testDBName,
						SystemID: "tes-sys-id",
					}, nil
				},
				StartReplicationFn: func(ctx context.Context, cfg pglib.ReplicationConfig) error {
					require.Equal(t, testLSN, cfg.StartPos)
					require.Equal(t, testSlot, cfg.SlotName)
					require.Equal(t, pluginArguments, cfg.PluginArguments)
					return nil
				},
				SendStandbyStatusUpdateFn: func(ctx context.Context, lsn uint64) error {
					require.Equal(t, testLSN, lsn)
					return nil
				},
			},
			connBuilder: func() (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "select restart_lsn from pg_replication_slots where slot_name=$1":
							require.Len(t, dest, 1)
							lsn, ok := dest[0].(*string)
							require.True(t, ok)
							*lsn = testLSNStr
							return nil
						case "select confirmed_flush_lsn from pg_replication_slots where slot_name=$1":
							require.Len(t, dest, 1)
							lsn, ok := dest[0].(*string)
							require.True(t, ok)
							*lsn = "0/0"
							return nil
						default:
							return fmt.Errorf("unexpected query: %s", query)
						}
					},
					CloseFn: func(ctx context.Context) error { return nil },
				}, nil
			},
			slotName: testSlot,

			wantErr: nil,
		},
		{
			name: "error - creating connection",
			replicationConn: &pgmocks.ReplicationConn{
				IdentifySystemFn: func(ctx context.Context) (pglib.IdentifySystemResult, error) {
					return pglib.IdentifySystemResult{
						DBName:   testDBName,
						SystemID: "tes-sys-id",
					}, nil
				},
			},
			connBuilder: func() (pglib.Querier, error) {
				return nil, errTest
			},
			slotName: testSlot,

			wantErr: errTest,
		},
		{
			name: "error - getting last synced LSN",
			replicationConn: &pgmocks.ReplicationConn{
				IdentifySystemFn: func(ctx context.Context) (pglib.IdentifySystemResult, error) {
					return pglib.IdentifySystemResult{
						DBName:   testDBName,
						SystemID: "tes-sys-id",
					}, nil
				},
			},
			connBuilder: func() (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "select confirmed_flush_lsn from pg_replication_slots where slot_name=$1":
							return errTest
						default:
							return fmt.Errorf("unexpected query: %s", query)
						}
					},
					CloseFn: func(ctx context.Context) error { return nil },
				}, nil
			},
			slotName: testSlot,

			wantErr: errTest,
		},
		{
			name: "error - getting restart LSN",
			replicationConn: &pgmocks.ReplicationConn{
				IdentifySystemFn: func(ctx context.Context) (pglib.IdentifySystemResult, error) {
					return pglib.IdentifySystemResult{
						DBName:   testDBName,
						SystemID: "tes-sys-id",
					}, nil
				},
			},
			connBuilder: func() (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "select restart_lsn from pg_replication_slots where slot_name=$1":
							return errTest
						case "select confirmed_flush_lsn from pg_replication_slots where slot_name=$1":
							require.Len(t, dest, 1)
							lsn, ok := dest[0].(*string)
							require.True(t, ok)
							*lsn = "0/0"
							return nil
						default:
							return fmt.Errorf("unexpected query: %s", query)
						}
					},
					CloseFn: func(ctx context.Context) error { return nil },
				}, nil
			},
			slotName: testSlot,

			wantErr: errTest,
		},
		{
			name: "error - starting replication",
			replicationConn: &pgmocks.ReplicationConn{
				IdentifySystemFn: func(ctx context.Context) (pglib.IdentifySystemResult, error) {
					return pglib.IdentifySystemResult{
						DBName:   testDBName,
						SystemID: "tes-sys-id",
					}, nil
				},
				StartReplicationFn: func(ctx context.Context, cfg pglib.ReplicationConfig) error {
					return errTest
				},
			},
			connBuilder: func() (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "select confirmed_flush_lsn from pg_replication_slots where slot_name=$1":
							require.Len(t, dest, 1)
							lsn, ok := dest[0].(*string)
							require.True(t, ok)
							*lsn = testLSNStr
							return nil
						default:
							return fmt.Errorf("unexpected query: %s", query)
						}
					},
					CloseFn: func(ctx context.Context) error { return nil },
				}, nil
			},
			slotName: testSlot,

			wantErr: errTest,
		},
		{
			name: "error - syncing LSN",
			replicationConn: &pgmocks.ReplicationConn{
				IdentifySystemFn: func(ctx context.Context) (pglib.IdentifySystemResult, error) {
					return pglib.IdentifySystemResult{
						DBName:   testDBName,
						SystemID: "tes-sys-id",
					}, nil
				},
				StartReplicationFn: func(ctx context.Context, cfg pglib.ReplicationConfig) error {
					require.Equal(t, testLSN, cfg.StartPos)
					require.Equal(t, testSlot, cfg.SlotName)
					require.Equal(t, pluginArguments, cfg.PluginArguments)
					return nil
				},
				SendStandbyStatusUpdateFn: func(ctx context.Context, lsn uint64) error {
					return errTest
				},
			},
			connBuilder: func() (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "select confirmed_flush_lsn from pg_replication_slots where slot_name=$1":
							require.Len(t, dest, 1)
							lsn, ok := dest[0].(*string)
							require.True(t, ok)
							*lsn = testLSNStr
							return nil
						default:
							return fmt.Errorf("unexpected query: %s", query)
						}
					},
					CloseFn: func(ctx context.Context) error { return nil },
				}, nil
			},
			slotName: testSlot,

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := Handler{
				logger:                log.NewNoopLogger(),
				pgReplicationConn:     tc.replicationConn,
				pgConnBuilder:         tc.connBuilder,
				pgReplicationSlotName: tc.slotName,
				lsnParser:             NewLSNParser(),
				logFields:             log.Fields{},
			}

			err := h.StartReplication(context.Background())
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestHandler_ReceiveMessage(t *testing.T) {
	t.Parallel()

	testData := []byte("test-data")

	tests := []struct {
		name            string
		replicationConn pglib.ReplicationQuerier
		includeTables   []string
		excludeTables   []string

		wantMessage *replication.Message
		wantErr     error
	}{
		{
			name: "ok",
			replicationConn: &pgmocks.ReplicationConn{
				ReceiveMessageFn: func(ctx context.Context) (*pglib.ReplicationMessage, error) {
					return &pglib.ReplicationMessage{
						LSN:            testLSN,
						ServerTime:     now,
						WALData:        testData,
						ReplyRequested: false,
					}, nil
				},
			},

			wantMessage: &replication.Message{
				LSN:            replication.LSN(testLSN),
				Data:           testData,
				ServerTime:     now,
				ReplyRequested: false,
			},
			wantErr: nil,
		},
		{
			name: "ok - receiving message, warning notice",
			replicationConn: &pgmocks.ReplicationConn{
				ReceiveMessageFn: func(ctx context.Context) (*pglib.ReplicationMessage, error) {
					return nil, &pglib.Error{Severity: "WARNING"}
				},
			},

			wantMessage: nil,
			wantErr:     nil,
		},
		{
			name: "ok - receiving message, warning notice no tuple identifier for table in excluded tables",
			replicationConn: &pgmocks.ReplicationConn{
				ReceiveMessageFn: func(ctx context.Context) (*pglib.ReplicationMessage, error) {
					return nil, &pglib.Error{Severity: "WARNING", Msg: "no tuple identifier for DELETE in table \"test_schema\".\"ignore_table\""}
				},
			},
			includeTables: []string{"test_schema.test_table"},
			excludeTables: []string{"test_schema.ignore_table"},

			wantMessage: nil,
			wantErr:     nil,
		},
		{
			name: "ok - receiving message, warning notice no tuple identifier for table not in included tables",
			replicationConn: &pgmocks.ReplicationConn{
				ReceiveMessageFn: func(ctx context.Context) (*pglib.ReplicationMessage, error) {
					return nil, &pglib.Error{Severity: "WARNING", Msg: "no tuple identifier for DELETE in table \"test_schema\".\"ignore_table\""}
				},
			},
			includeTables: []string{"test_schema.test_table"},

			wantMessage: nil,
			wantErr:     nil,
		},
		{
			name: "error - receiving message - timeout",
			replicationConn: &pgmocks.ReplicationConn{
				ReceiveMessageFn: func(ctx context.Context) (*pglib.ReplicationMessage, error) {
					return nil, pglib.ErrConnTimeout
				},
			},

			wantMessage: nil,
			wantErr:     replication.ErrConnTimeout,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			includeTables, err := pglib.NewSchemaTableMap(tc.includeTables)
			require.NoError(t, err)
			excludeTables, err := pglib.NewSchemaTableMap(tc.excludeTables)
			require.NoError(t, err)

			h := Handler{
				logger:            log.NewNoopLogger(),
				pgReplicationConn: tc.replicationConn,
				includedTables:    includeTables,
				excludedTables:    excludeTables,
			}

			msg, err := h.ReceiveMessage(context.Background())
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantMessage, msg)
		})
	}
}

func TestHandler_GetReplicationLag(t *testing.T) {
	t.Parallel()

	testLag := int64(5)

	tests := []struct {
		name        string
		connBuilder func() (pglib.Querier, error)

		wantLag int64
		wantErr error
	}{
		{
			name: "ok",
			connBuilder: func() (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "SELECT (pg_current_wal_lsn() - confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name=$1":
							require.Len(t, dest, 1)
							lag, ok := dest[0].(*int64)
							require.True(t, ok)
							*lag = testLag
							return nil
						default:
							return fmt.Errorf("unexpected query: %s", query)
						}
					},
					CloseFn: func(ctx context.Context) error { return nil },
				}, nil
			},

			wantLag: testLag,
			wantErr: nil,
		},
		{
			name: "error - building connection",
			connBuilder: func() (pglib.Querier, error) {
				return nil, errTest
			},

			wantLag: -1,
			wantErr: errTest,
		},
		{
			name: "error - getting lag",
			connBuilder: func() (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						return errTest
					},
					CloseFn: func(ctx context.Context) error { return nil },
				}, nil
			},

			wantLag: -1,
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := Handler{
				logger:                log.NewNoopLogger(),
				pgConnBuilder:         tc.connBuilder,
				pgReplicationSlotName: testSlot,
				lsnParser:             NewLSNParser(),
			}

			lag, err := h.GetReplicationLag(context.Background())
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantLag, lag)
		})
	}
}

func TestHandler_GetCurrentLSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		connBuilder func() (pglib.Querier, error)

		wantLSN replication.LSN
		wantErr error
	}{
		{
			name: "ok",
			connBuilder: func() (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name=$1":
							require.Len(t, dest, 1)
							lsn, ok := dest[0].(*string)
							require.True(t, ok)
							*lsn = testLSNStr
							return nil
						default:
							return fmt.Errorf("unexpected query: %s", query)
						}
					},
					CloseFn: func(ctx context.Context) error { return nil },
				}, nil
			},

			wantLSN: replication.LSN(testLSN),
			wantErr: nil,
		},
		{
			name: "error - building connection",
			connBuilder: func() (pglib.Querier, error) {
				return nil, errTest
			},

			wantLSN: replication.LSN(0),
			wantErr: errTest,
		},
		{
			name: "error - getting current lsn",
			connBuilder: func() (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						return errTest
					},
					CloseFn: func(ctx context.Context) error { return nil },
				}, nil
			},

			wantLSN: replication.LSN(0),
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := Handler{
				logger:                log.NewNoopLogger(),
				pgConnBuilder:         tc.connBuilder,
				pgReplicationSlotName: testSlot,
				lsnParser:             NewLSNParser(),
			}

			lsn, err := h.GetCurrentLSN(context.Background())
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantLSN, lsn)
		})
	}
}

func TestHandler_verifyReplicationSlotExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		connBuilder func() (pglib.Querier, error)
		slotExists  bool
		wantErr     error
	}{
		{
			name: "ok - slot exists",
			connBuilder: func() (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)":
							require.Len(t, dest, 1)
							exists, ok := dest[0].(*bool)
							require.True(t, ok)
							*exists = true
							return nil
						default:
							return fmt.Errorf("unexpected query: %s", query)
						}
					},
					CloseFn: func(ctx context.Context) error { return nil },
				}, nil
			},
			slotExists: true,
			wantErr:    nil,
		},
		{
			name: "error - slot does not exist",
			connBuilder: func() (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)":
							require.Len(t, dest, 1)
							exists, ok := dest[0].(*bool)
							require.True(t, ok)
							*exists = false
							return nil
						default:
							return fmt.Errorf("unexpected query: %s", query)
						}
					},
					CloseFn: func(ctx context.Context) error { return nil },
				}, nil
			},
			slotExists: false,
			wantErr:    fmt.Errorf("replication slot %q does not exist", testSlot),
		},
		{
			name: "error - building connection",
			connBuilder: func() (pglib.Querier, error) {
				return nil, errTest
			},
			slotExists: false,
			wantErr:    errTest,
		},
		{
			name: "error - query execution",
			connBuilder: func() (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						return errTest
					},
					CloseFn: func(ctx context.Context) error { return nil },
				}, nil
			},
			slotExists: false,
			wantErr:    fmt.Errorf("retrieving replication slot: %w", errTest),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := Handler{
				logger:                log.NewNoopLogger(),
				pgConnBuilder:         tc.connBuilder,
				pgReplicationSlotName: testSlot,
			}

			err := h.verifyReplicationSlotExists(context.Background())
			if !errors.Is(err, tc.wantErr) {
				require.Equal(t, tc.wantErr, err)
			}
		})
	}
}
