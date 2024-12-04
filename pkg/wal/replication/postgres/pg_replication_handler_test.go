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
		replicationConn pgReplicationConn
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
					QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "select restart_lsn from pg_replication_slots where slot_name=$1":
							return &mockRow{scanFn: func(args ...any) error { return errors.New("restart lsn should not be called") }}
						case "select confirmed_flush_lsn from pg_replication_slots where slot_name=$1":
							return &mockRow{lsn: testLSNStr}
						default:
							return &mockRow{scanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %s", query) }}
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
					QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "select restart_lsn from pg_replication_slots where slot_name=$1":
							return &mockRow{lsn: testLSNStr}
						case "select confirmed_flush_lsn from pg_replication_slots where slot_name=$1":
							return &mockRow{lsn: "0/0"}
						default:
							return &mockRow{scanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %s", query) }}
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
					QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "select confirmed_flush_lsn from pg_replication_slots where slot_name=$1":
							return &mockRow{scanFn: func(args ...any) error { return errTest }}
						default:
							return &mockRow{scanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %s", query) }}
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
					QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "select restart_lsn from pg_replication_slots where slot_name=$1":
							return &mockRow{scanFn: func(args ...any) error { return errTest }}
						case "select confirmed_flush_lsn from pg_replication_slots where slot_name=$1":
							return &mockRow{lsn: "0/0"}
						default:
							return &mockRow{scanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %s", query) }}
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
					QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "select confirmed_flush_lsn from pg_replication_slots where slot_name=$1":
							return &mockRow{lsn: testLSNStr}
						default:
							return &mockRow{scanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %s", query) }}
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
					QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "select confirmed_flush_lsn from pg_replication_slots where slot_name=$1":
							return &mockRow{lsn: testLSNStr}
						default:
							return &mockRow{scanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %s", query) }}
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := Handler{
				logger:                log.NewNoopLogger(),
				pgReplicationConn:     tc.replicationConn,
				pgConnBuilder:         tc.connBuilder,
				pgReplicationSlotName: tc.slotName,
				lsnParser:             NewLSNParser(),
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
		replicationConn pgReplicationConn

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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := Handler{
				logger:            log.NewNoopLogger(),
				pgReplicationConn: tc.replicationConn,
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
					QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "SELECT (pg_current_wal_lsn() - confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name=$1":
							return &mockRow{lag: testLag}
						default:
							return &mockRow{scanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %s", query) }}
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
					QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
						return &mockRow{scanFn: func(args ...any) error { return errTest }}
					},
					CloseFn: func(ctx context.Context) error { return nil },
				}, nil
			},

			wantLag: -1,
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
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
					QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
						require.Len(t, args, 1)
						require.Equal(t, args[0], testSlot)
						switch query {
						case "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name=$1":
							return &mockRow{lsn: testLSNStr}
						default:
							return &mockRow{scanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %s", query) }}
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
					QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
						return &mockRow{scanFn: func(args ...any) error { return errTest }}
					},
					CloseFn: func(ctx context.Context) error { return nil },
				}, nil
			},

			wantLSN: replication.LSN(0),
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
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
