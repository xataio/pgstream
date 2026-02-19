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

	testSchema := "test-schema"
	testTables := []string{"table1", "table2"}
	testSnapshot := snapshot.Snapshot{
		SchemaTables: map[string][]string{
			testSchema: testTables,
		},
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
						Schema: testSchema,
						Tables: testTables,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusCompleted,
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
						{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusCompleted,
						},
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

			wantErr: errTest,
		},
		{
			name: "error - snapshot error on wrapped generator",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					require.Equal(t, &snapshot.Request{
						Schema: testSchema,
						Tables: testTables,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusCompleted,
							Errors: snapshot.NewSchemaErrors(testSchema, errTest),
						}, r)
						return nil
					default:
						return fmt.Errorf("unexpected call to UpdateSnapshotRequestFn: %d", i)

					}
				},
			},
			generator: &mockGenerator{
				createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					return errTest
				},
			},

			wantErr: errTest,
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

			wantErr: errTest,
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
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusInProgress,
						}, r)
						return errTest
					default:
						return fmt.Errorf("unexpected call to UpdateSnapshotRequestFn: %d", i)

					}
				},
			},
			generator: &mockGenerator{},

			wantErr: errTest,
		},
		{
			name: "error - updating snapshot request completed without errors",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					require.Equal(t, &snapshot.Request{
						Schema: testSchema,
						Tables: testTables,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusCompleted,
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

			wantErr: errTest,
		},
		{
			name: "error - updating snapshot request completed with snapshot and table errors",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					require.Equal(t, &snapshot.Request{
						Schema: testSchema,
						Tables: testTables,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusCompleted,
							Errors: &snapshot.SchemaErrors{
								Schema:       testSchema,
								GlobalErrors: []string{errTest.Error()},
								TableErrors: map[string]string{
									"table1": errTest.Error(),
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
						testSchema: &snapshot.SchemaErrors{
							Schema:       testSchema,
							GlobalErrors: []string{errTest.Error()},
							TableErrors: map[string]string{
								"table1": errTest.Error(),
							},
						},
					}
				},
			},

			wantErr: &snapshot.Errors{
				testSchema: &snapshot.SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{errTest.Error(), updateErr.Error()},
					TableErrors: map[string]string{
						"table1": errTest.Error(),
					},
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
						Schema: testSchema,
						Tables: testTables,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusCompleted,
							Errors: &snapshot.SchemaErrors{
								Schema: testSchema,
								TableErrors: map[string]string{
									"table1": errTest.Error(),
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
						testSchema: &snapshot.SchemaErrors{
							Schema: testSchema,
							TableErrors: map[string]string{
								"table1": errTest.Error(),
							},
						},
					}
				},
			},

			wantErr: &snapshot.Errors{
				testSchema: &snapshot.SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{updateErr.Error()},
					TableErrors: map[string]string{
						"table1": errTest.Error(),
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sr := NewSnapshotRecorder(&Config{
				RepeatableSnapshots: false,
				SchemaWorkers:       1,
			}, tc.store, tc.generator)
			defer sr.Close()

			err := sr.CreateSnapshot(context.Background(), newTestSnapshot())
			if !errors.Is(err, tc.wantErr) {
				require.Equal(t, tc.wantErr, err)
			}
		})
	}
}

func TestSnapshotRecorder_filterOutExistingSnapshots(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	testTables := []string{"table1", "table2"}
	errTest := errors.New("oh noes")

	tests := []struct {
		name                string
		store               snapshotstore.Store
		tables              []string
		repeatableSnapshots bool

		wantTables map[string][]string
		wantErr    error
	}{
		{
			name: "ok - no existing snapshots",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
			},
			wantTables: map[string][]string{
				testSchema: testTables,
			},
		},
		{
			name: "ok - no existing snapshots with wildcard",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: []string{"table2"},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			tables: []string{"*"},
			wantTables: map[string][]string{
				testSchema: {"*"},
			},
		},
		{
			name: "ok - existing snapshots",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: []string{"table2"},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			wantTables: map[string][]string{
				testSchema: {"table1"},
			},
		},
		{
			name: "ok - existing snapshots with repeatable snapshots",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return nil, errors.New("unexpected call to GetSnapshotRequestsBySchemaFn")
				},
			},
			repeatableSnapshots: true,
			wantTables: map[string][]string{
				testSchema: {"table1", "table2"},
			},
		},
		{
			name: "ok - existing wildcard snapshot with wildcard table",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: []string{"*"},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			tables:     []string{"*"},
			wantTables: map[string][]string{},
		},
		{
			name: "ok - existing wildcard snapshot",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: []string{"*"},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			wantTables: map[string][]string{},
		},
		{
			name: "ok - existing failed snapshots",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: []string{"table2"},
							Errors: snapshot.NewSchemaErrors(testSchema, errTest),
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			wantTables: map[string][]string{
				testSchema: {"table1", "table2"},
			},
		},
		{
			name: "ok - existing failed wildcard snapshot",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: []string{"*"},
							Errors: &snapshot.SchemaErrors{
								Schema: testSchema,
								TableErrors: map[string]string{
									"table2": errTest.Error(),
								},
							},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			tables: []string{"*"},
			wantTables: map[string][]string{
				testSchema: {"table2"},
			},
		},
		{
			name: "ok - existing failed table on wildcard snapshot",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: []string{"*"},
							Errors: &snapshot.SchemaErrors{
								Schema: testSchema,
								TableErrors: map[string]string{
									"table2": errTest.Error(),
								},
							},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			wantTables: map[string][]string{
				testSchema: {"table2"},
			},
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
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sr := SnapshotRecorder{
				store:               tc.store,
				repeatableSnapshots: tc.repeatableSnapshots,
			}

			tables := testTables
			if tc.tables != nil {
				tables = tc.tables
			}

			filteredTables, err := sr.filterOutExistingSnapshots(context.Background(), map[string][]string{testSchema: tables})
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantTables, filteredTables)
		})
	}
}
