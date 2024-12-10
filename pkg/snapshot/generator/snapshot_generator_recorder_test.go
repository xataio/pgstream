// SPDX-License-Identifier: Apache-2.0

package generator

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/snapshot"
	snapshotstore "github.com/xataio/pgstream/pkg/snapshot/store"
	snapshotstoremocks "github.com/xataio/pgstream/pkg/snapshot/store/mocks"
)

func TestSnapshotRecorder_CreateSnapshot(t *testing.T) {
	t.Parallel()

	testSnapshot := snapshot.Snapshot{
		SchemaName: "test-schema",
		TableNames: []string{"table1", "table2"},
	}

	newTestSnapshot := func() *snapshot.Snapshot {
		ss := testSnapshot
		return &ss
	}

	errTest := errors.New("oh noes")
	updateErr := errors.New("update error")

	tests := []struct {
		name      string
		store     snapshotstore.Store
		generator SnapshotGenerator

		wantErr error
	}{
		{
			name: "ok",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					require.Equal(t, &snapshot.Request{
						Snapshot: testSnapshot,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Snapshot: testSnapshot,
							Status:   snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Snapshot: testSnapshot,
							Status:   snapshot.StatusCompleted,
						}, r)
						return nil
					default:
						return fmt.Errorf("unexpected call to UpdateSnapshotRequestFn: %d", i)

					}
				},
			},
			generator: &mockGenerator{
				createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					require.Equal(t, newTestSnapshot(), ss)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - all tables filtered out",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{Snapshot: testSnapshot, Status: snapshot.StatusCompleted},
					}, nil
				},
			},
			generator: &mockGenerator{},

			wantErr: nil,
		},
		{
			name: "error - getting existing requests",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return nil, errTest
				},
			},
			generator: &mockGenerator{},

			wantErr: &snapshot.Errors{Snapshot: fmt.Errorf("retrieving existing snapshots for schema: %w", errTest)},
		},
		{
			name: "error - snapshot error on wrapped generator",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					require.Equal(t, &snapshot.Request{
						Snapshot: testSnapshot,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Snapshot: testSnapshot,
							Status:   snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Snapshot: testSnapshot,
							Status:   snapshot.StatusCompleted,
							Errors:   &snapshot.Errors{Snapshot: errTest},
						}, r)
						return nil
					default:
						return fmt.Errorf("unexpected call to UpdateSnapshotRequestFn: %d", i)

					}
				},
			},
			generator: &mockGenerator{
				createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					return &snapshot.Errors{Snapshot: errTest}
				},
			},

			wantErr: &snapshot.Errors{Snapshot: errTest},
		},
		{
			name: "error - recording snapshot request",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					return errTest
				},
			},
			generator: &mockGenerator{
				createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					return errors.New("createSnapshotFn: should not be called")
				},
			},

			wantErr: &snapshot.Errors{Snapshot: errTest},
		},
		{
			name: "error - updating snapshot request in progress",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Snapshot: testSnapshot,
							Status:   snapshot.StatusInProgress,
						}, r)
						return errTest
					default:
						return fmt.Errorf("unexpected call to UpdateSnapshotRequestFn: %d", i)

					}
				},
			},
			generator: &mockGenerator{},

			wantErr: &snapshot.Errors{Snapshot: errTest},
		},
		{
			name: "error - updating snapshot request completed without errors",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					require.Equal(t, &snapshot.Request{
						Snapshot: testSnapshot,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Snapshot: testSnapshot,
							Status:   snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Snapshot: testSnapshot,
							Status:   snapshot.StatusCompleted,
						}, r)
						return errTest
					default:
						return fmt.Errorf("unexpected call to UpdateSnapshotRequestFn: %d", i)

					}
				},
			},
			generator: &mockGenerator{
				createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					require.Equal(t, newTestSnapshot(), ss)
					return nil
				},
			},

			wantErr: &snapshot.Errors{Snapshot: errTest},
		},
		{
			name: "error - updating snapshot request completed with snapshot and table errors",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					require.Equal(t, &snapshot.Request{
						Snapshot: testSnapshot,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Snapshot: testSnapshot,
							Status:   snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Snapshot: testSnapshot,
							Status:   snapshot.StatusCompleted,
							Errors: &snapshot.Errors{
								Snapshot: errTest,
								Tables: []snapshot.TableError{
									{Table: "table1", ErrorMsg: errTest.Error()},
								},
							},
						}, r)
						return updateErr
					default:
						return fmt.Errorf("unexpected call to UpdateSnapshotRequestFn: %d", i)

					}
				},
			},
			generator: &mockGenerator{
				createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					require.Equal(t, newTestSnapshot(), ss)
					return &snapshot.Errors{
						Snapshot: errTest,
						Tables: []snapshot.TableError{
							{Table: "table1", ErrorMsg: errTest.Error()},
						},
					}
				},
			},

			wantErr: &snapshot.Errors{
				Snapshot: errors.Join(errTest, updateErr),
				Tables: []snapshot.TableError{
					{Table: "table1", ErrorMsg: errTest.Error()},
				},
			},
		},
		{
			name: "error - updating snapshot request completed with table errors",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					require.Equal(t, &snapshot.Request{
						Snapshot: testSnapshot,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Snapshot: testSnapshot,
							Status:   snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Snapshot: testSnapshot,
							Status:   snapshot.StatusCompleted,
							Errors: &snapshot.Errors{
								Tables: []snapshot.TableError{
									{Table: "table1", ErrorMsg: errTest.Error()},
								},
							},
						}, r)
						return updateErr
					default:
						return fmt.Errorf("unexpected call to UpdateSnapshotRequestFn: %d", i)

					}
				},
			},
			generator: &mockGenerator{
				createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					require.Equal(t, newTestSnapshot(), ss)
					return &snapshot.Errors{
						Tables: []snapshot.TableError{
							{Table: "table1", ErrorMsg: errTest.Error()},
						},
					}
				},
			},

			wantErr: &snapshot.Errors{
				Snapshot: updateErr,
				Tables: []snapshot.TableError{
					{Table: "table1", ErrorMsg: errTest.Error()},
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sr := NewSnapshotRecorder(tc.store, tc.generator)
			defer sr.Close()

			err := sr.CreateSnapshot(context.Background(), newTestSnapshot())
			require.Equal(t, tc.wantErr, err)
		})
	}
}

func TestSnapshotRecorder_filterOutExistingSnapshots(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	testTables := []string{"table1", "table2"}
	errTest := errors.New("oh noes")

	tests := []struct {
		name   string
		store  snapshotstore.Store
		tables []string

		wantTables []string
		wantErr    error
	}{
		{
			name: "ok - no existing snapshots",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
			},
			wantTables: testTables,
		},
		{
			name: "ok - no existing snapshots with wildcard",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Snapshot: snapshot.Snapshot{
								SchemaName: testSchema,
								TableNames: []string{"table2"},
							},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			tables:     []string{"*"},
			wantTables: []string{"*"},
		},
		{
			name: "ok - existing snapshots",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Snapshot: snapshot.Snapshot{
								SchemaName: testSchema,
								TableNames: []string{"table2"},
							},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			wantTables: []string{"table1"},
		},
		{
			name: "ok - existing wildcard snapshot with wildcard table",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Snapshot: snapshot.Snapshot{
								SchemaName: testSchema,
								TableNames: []string{"*"},
							},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			tables:     []string{"*"},
			wantTables: []string{},
		},
		{
			name: "ok - existing wildcard snapshot",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Snapshot: snapshot.Snapshot{
								SchemaName: testSchema,
								TableNames: []string{"*"},
							},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			wantTables: []string{},
		},
		{
			name: "ok - existing failed snapshots",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Snapshot: snapshot.Snapshot{
								SchemaName: testSchema,
								TableNames: []string{"table2"},
							},
							Errors: &snapshot.Errors{Snapshot: errTest},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			wantTables: []string{"table1", "table2"},
		},
		{
			name: "ok - existing failed wildcard snapshot",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Snapshot: snapshot.Snapshot{
								SchemaName: testSchema,
								TableNames: []string{"*"},
							},
							Errors: &snapshot.Errors{Tables: []snapshot.TableError{{Table: "table2", ErrorMsg: errTest.Error()}}},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			tables:     []string{"*"},
			wantTables: []string{"table2"},
		},
		{
			name: "ok - existing failed table on wildcard snapshot",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Snapshot: snapshot.Snapshot{
								SchemaName: testSchema,
								TableNames: []string{"*"},
							},
							Errors: &snapshot.Errors{Tables: []snapshot.TableError{{Table: "table2", ErrorMsg: errTest.Error()}}},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			wantTables: []string{"table2"},
		},
		{
			name: "error - retrieving existing snapshots",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return nil, errTest
				},
			},
			wantTables: nil,
			wantErr:    errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sr := SnapshotRecorder{
				store: tc.store,
			}

			tables := testTables
			if tc.tables != nil {
				tables = tc.tables
			}

			filteredTables, err := sr.filterOutExistingSnapshots(context.Background(), testSchema, tables)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantTables, filteredTables)
		})
	}
}
