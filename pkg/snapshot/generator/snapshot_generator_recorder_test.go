// SPDX-License-Identifier: Apache-2.0

package generator

import (
	"context"
	"errors"
	"fmt"
	"slices"
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
						Mode:   snapshot.RequestModeData,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Mode:   snapshot.RequestModeData,
							Status: snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Mode:   snapshot.RequestModeData,
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
						Mode:   snapshot.RequestModeData,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Mode:   snapshot.RequestModeData,
							Status: snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Mode:   snapshot.RequestModeData,
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
							Mode:   snapshot.RequestModeData,
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
						Mode:   snapshot.RequestModeData,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Mode:   snapshot.RequestModeData,
							Status: snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Mode:   snapshot.RequestModeData,
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
						Mode:   snapshot.RequestModeData,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Mode:   snapshot.RequestModeData,
							Status: snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Mode:   snapshot.RequestModeData,
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
						Mode:   snapshot.RequestModeData,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Mode:   snapshot.RequestModeData,
							Status: snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Mode:   snapshot.RequestModeData,
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
				SnapshotWorkers:     1,
			}, tc.store, tc.generator)
			defer sr.Close()

			err := sr.CreateSnapshot(context.Background(), newTestSnapshot())
			if !errors.Is(err, tc.wantErr) {
				require.Equal(t, tc.wantErr, err)
			}
		})
	}
}

func TestSnapshotRecorder_CreateSnapshot_schemaOnlyTables(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	testTables := []string{"table1", "table2"}

	t.Run("schema-only tables reach the wrapped generator and are recorded", func(t *testing.T) {
		t.Parallel()

		// even when all data tables have already been snapshotted, schema-only
		// tables that haven't been recorded yet must still reach the wrapped
		// generator, and be recorded so that restarts can skip them
		var recordedRequest *snapshot.Request
		store := &snapshotstoremocks.Store{
			GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
				return []*snapshot.Request{
					{
						Schema: testSchema,
						Tables: testTables,
						Status: snapshot.StatusCompleted,
					},
				}, nil
			},
			CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
				recordedRequest = r
				return nil
			},
			UpdateSnapshotRequestFn: func(ctx context.Context, _ uint, r *snapshot.Request) error {
				return nil
			},
		}

		wrappedCalled := false
		generator := &mockGenerator{
			createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
				wrappedCalled = true
				require.Empty(t, ss.SchemaTables)
				require.Equal(t, map[string][]string{testSchema: {"audit_log"}}, ss.SchemaOnlyTables)
				return nil
			},
		}

		sr := NewSnapshotRecorder(&Config{
			RepeatableSnapshots: false,
			SnapshotWorkers:     1,
		}, store, generator)
		defer sr.Close()

		err := sr.CreateSnapshot(context.Background(), &snapshot.Snapshot{
			SchemaTables: map[string][]string{
				testSchema: testTables,
			},
			SchemaOnlyTables: map[string][]string{
				testSchema: {"audit_log"},
			},
		})
		require.NoError(t, err)
		require.True(t, wrappedCalled)
		require.NotNil(t, recordedRequest)
		require.Equal(t, testSchema, recordedRequest.Schema)
		require.Equal(t, []string{"audit_log"}, recordedRequest.Tables)
		require.Equal(t, snapshot.RequestModeSchemaOnly, recordedRequest.GetMode())
	})

	t.Run("already recorded schema-only tables are skipped on restart", func(t *testing.T) {
		t.Parallel()

		store := &snapshotstoremocks.Store{
			GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
				return []*snapshot.Request{
					{
						Schema: testSchema,
						Tables: slices.Clone(testTables),
						Status: snapshot.StatusCompleted,
						Mode:   snapshot.RequestModeData,
					},
					{
						Schema: testSchema,
						Tables: []string{"audit_log"},
						Status: snapshot.StatusCompleted,
						Mode:   snapshot.RequestModeSchemaOnly,
					},
				}, nil
			},
			CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
				return fmt.Errorf("unexpected call to CreateSnapshotRequestFn: %v", r)
			},
		}

		generator := &mockGenerator{
			createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
				return fmt.Errorf("unexpected call to wrapped generator: %v", ss)
			},
		}

		sr := NewSnapshotRecorder(&Config{
			RepeatableSnapshots: false,
			SnapshotWorkers:     1,
		}, store, generator)
		defer sr.Close()

		err := sr.CreateSnapshot(context.Background(), &snapshot.Snapshot{
			SchemaTables: map[string][]string{
				testSchema: testTables,
			},
			SchemaOnlyTables: map[string][]string{
				testSchema: {"audit_log"},
			},
		})
		require.NoError(t, err)
	})

	t.Run("promoted schema-only table is data snapshotted (explicit list)", func(t *testing.T) {
		t.Parallel()

		// audit_log was snapshotted as schema-only, then moved to the data
		// tables list: its schema-only record must not suppress the data copy
		store := &snapshotstoremocks.Store{
			GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
				return []*snapshot.Request{
					{
						Schema: testSchema,
						Tables: slices.Clone(testTables),
						Status: snapshot.StatusCompleted,
						Mode:   snapshot.RequestModeData,
					},
					{
						Schema: testSchema,
						Tables: []string{"audit_log"},
						Status: snapshot.StatusCompleted,
						Mode:   snapshot.RequestModeSchemaOnly,
					},
				}, nil
			},
			CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
				return nil
			},
			UpdateSnapshotRequestFn: func(ctx context.Context, _ uint, r *snapshot.Request) error {
				return nil
			},
		}

		wrappedCalled := false
		generator := &mockGenerator{
			createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
				wrappedCalled = true
				require.Equal(t, map[string][]string{testSchema: {"audit_log"}}, ss.SchemaTables)
				require.Empty(t, ss.SchemaOnlyTables)
				return nil
			},
		}

		sr := NewSnapshotRecorder(&Config{SnapshotWorkers: 1}, store, generator)
		defer sr.Close()

		err := sr.CreateSnapshot(context.Background(), &snapshot.Snapshot{
			SchemaTables: map[string][]string{
				testSchema: append(slices.Clone(testTables), "audit_log"),
			},
		})
		require.NoError(t, err)
		require.True(t, wrappedCalled)
	})

	t.Run("promoted schema-only table is data snapshotted (wildcard list)", func(t *testing.T) {
		t.Parallel()

		// audit_log's data was skipped during the completed wildcard data
		// snapshot; once it's no longer schema-only, the wildcard coverage
		// must not hide it
		store := &snapshotstoremocks.Store{
			GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
				return []*snapshot.Request{
					{
						Schema: testSchema,
						Tables: []string{"*"},
						Status: snapshot.StatusCompleted,
						Mode:   snapshot.RequestModeData,
					},
					{
						Schema: testSchema,
						Tables: []string{"audit_log"},
						Status: snapshot.StatusCompleted,
						Mode:   snapshot.RequestModeSchemaOnly,
					},
				}, nil
			},
			CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
				return nil
			},
			UpdateSnapshotRequestFn: func(ctx context.Context, _ uint, r *snapshot.Request) error {
				return nil
			},
		}

		wrappedCalled := false
		generator := &mockGenerator{
			createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
				wrappedCalled = true
				require.Equal(t, map[string][]string{testSchema: {"audit_log"}}, ss.SchemaTables)
				return nil
			},
		}

		sr := NewSnapshotRecorder(&Config{SnapshotWorkers: 1}, store, generator)
		defer sr.Close()

		err := sr.CreateSnapshot(context.Background(), &snapshot.Snapshot{
			SchemaTables: map[string][]string{
				testSchema: {"*"},
			},
		})
		require.NoError(t, err)
		require.True(t, wrappedCalled)
	})

	t.Run("wildcard coverage rule skips tables still configured as schema-only", func(t *testing.T) {
		t.Parallel()

		// stable config restart: audit_log has a schema-only record but is
		// still schema-only, so it must not be re-added to the data scope nor
		// re-run as schema-only
		store := &snapshotstoremocks.Store{
			GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
				return []*snapshot.Request{
					{
						Schema: testSchema,
						Tables: []string{"*"},
						Status: snapshot.StatusCompleted,
						Mode:   snapshot.RequestModeData,
					},
					{
						Schema: testSchema,
						Tables: []string{"audit_log"},
						Status: snapshot.StatusCompleted,
						Mode:   snapshot.RequestModeSchemaOnly,
					},
				}, nil
			},
			CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
				return fmt.Errorf("unexpected call to CreateSnapshotRequestFn: %v", r)
			},
		}

		generator := &mockGenerator{
			createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
				return fmt.Errorf("unexpected call to wrapped generator: %v", ss)
			},
		}

		sr := NewSnapshotRecorder(&Config{SnapshotWorkers: 1}, store, generator)
		defer sr.Close()

		err := sr.CreateSnapshot(context.Background(), &snapshot.Snapshot{
			SchemaTables: map[string][]string{
				testSchema: {"*"},
			},
			SchemaOnlyTables: map[string][]string{
				testSchema: {"audit_log"},
			},
		})
		require.NoError(t, err)
	})

	t.Run("demoted data table does not re-run the schema snapshot", func(t *testing.T) {
		t.Parallel()

		// audit_log was fully snapshotted as a data table, then moved to
		// schema_only_tables: its schema already exists on the target, so
		// nothing should run
		store := &snapshotstoremocks.Store{
			GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
				return []*snapshot.Request{
					{
						Schema: testSchema,
						Tables: append(slices.Clone(testTables), "audit_log"),
						Status: snapshot.StatusCompleted,
						Mode:   snapshot.RequestModeData,
					},
				}, nil
			},
			CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
				return fmt.Errorf("unexpected call to CreateSnapshotRequestFn: %v", r)
			},
		}

		generator := &mockGenerator{
			createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
				return fmt.Errorf("unexpected call to wrapped generator: %v", ss)
			},
		}

		sr := NewSnapshotRecorder(&Config{SnapshotWorkers: 1}, store, generator)
		defer sr.Close()

		err := sr.CreateSnapshot(context.Background(), &snapshot.Snapshot{
			SchemaTables: map[string][]string{
				testSchema: testTables,
			},
			SchemaOnlyTables: map[string][]string{
				testSchema: {"audit_log"},
			},
		})
		require.NoError(t, err)
	})
}

func TestSnapshotRecorder_createRequests(t *testing.T) {
	t.Parallel()

	sr := &SnapshotRecorder{}

	tests := []struct {
		name string
		ss   *snapshot.Snapshot

		wantRequests []*snapshot.Request
	}{
		{
			name: "data tables only",
			ss: &snapshot.Snapshot{
				SchemaTables: map[string][]string{"public": {"users"}},
			},
			wantRequests: []*snapshot.Request{
				{Schema: "public", Tables: []string{"users"}, Mode: snapshot.RequestModeData},
			},
		},
		{
			name: "data and schema-only tables in the same schema",
			ss: &snapshot.Snapshot{
				SchemaTables:     map[string][]string{"public": {"users"}},
				SchemaOnlyTables: map[string][]string{"public": {"audit_log"}},
			},
			wantRequests: []*snapshot.Request{
				{Schema: "public", Tables: []string{"users"}, Mode: snapshot.RequestModeData},
				{Schema: "public", Tables: []string{"audit_log"}, Mode: snapshot.RequestModeSchemaOnly},
			},
		},
		{
			name: "table explicitly listed in both lists is recorded as data only",
			ss: &snapshot.Snapshot{
				SchemaTables:     map[string][]string{"public": {"audit_log"}},
				SchemaOnlyTables: map[string][]string{"public": {"audit_log"}},
			},
			wantRequests: []*snapshot.Request{
				{Schema: "public", Tables: []string{"audit_log"}, Mode: snapshot.RequestModeData},
			},
		},
		{
			name: "data wildcard under schema-only wildcard is not recorded as data coverage",
			ss: &snapshot.Snapshot{
				SchemaTables:     map[string][]string{"public": {"*"}},
				SchemaOnlyTables: map[string][]string{"public": {"*"}},
			},
			wantRequests: []*snapshot.Request{
				{Schema: "public", Tables: []string{"*"}, Mode: snapshot.RequestModeSchemaOnly},
			},
		},
		{
			name: "explicit data table under schema-only wildcard keeps its data record",
			ss: &snapshot.Snapshot{
				SchemaTables:     map[string][]string{"public": {"*", "users"}},
				SchemaOnlyTables: map[string][]string{"public": {"*"}},
			},
			wantRequests: []*snapshot.Request{
				{Schema: "public", Tables: []string{"users"}, Mode: snapshot.RequestModeData},
				{Schema: "public", Tables: []string{"*"}, Mode: snapshot.RequestModeSchemaOnly},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			requests := sr.createRequests(tc.ss)
			require.ElementsMatch(t, tc.wantRequests, requests)
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

			ss := &snapshot.Snapshot{SchemaTables: map[string][]string{testSchema: tables}}
			err := sr.filterOutExistingSnapshots(context.Background(), ss)
			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantErr == nil {
				require.Equal(t, tc.wantTables, ss.SchemaTables)
			}
		})
	}
}
