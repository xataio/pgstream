// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/transformers"
	transformermocks "github.com/xataio/pgstream/pkg/transformers/mocks"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
)

func TestTransformer_New(t *testing.T) {
	t.Parallel()

	mockProcessor := &mocks.Processor{}
	testTransformer, err := transformers.NewStringTransformer(nil)
	require.NoError(t, err)

	mockBuilder := &transformermocks.TransformerBuilder{
		NewFn: func(_ *transformers.Config) (transformers.Transformer, error) {
			return testTransformer, nil
		},
	}

	testTransformerMap := map[string]ColumnTransformers{
		"\"public\".\"test1\"": {
			"column_1": testTransformer,
		},
		"\"test\".\"test2\"": {
			"column_2": testTransformer,
		},
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name    string
		config  *Config
		parseFn ParseFn

		wantTransformer *Transformer
		wantErr         error
	}{
		{
			name: "ok",
			config: &Config{
				ValidationMode: validationModeStrict,
				TransformerRules: []TableRules{
					{
						Schema: "public",
						Table:  "test1",
						ColumnRules: map[string]TransformerRules{
							"column_1": {
								Name: "string",
							},
						},
					},
					{
						Schema: "test",
						Table:  "test2",
						ColumnRules: map[string]TransformerRules{
							"column_2": {
								Name: "string",
							},
						},
					},
				},
			},

			wantTransformer: &Transformer{
				transformerMap:       testTransformerMap,
				validationMode:       validationModeStrict,
				tableValidationModes: map[string]string{},
			},
			wantErr: nil,
		},
		{
			name: "ok - with table level validation modes",
			config: &Config{
				ValidationMode: validationModeTableLevel,
				TransformerRules: []TableRules{
					{
						ValidationMode: validationModeStrict,
						Schema:         "public",
						Table:          "test1",
						ColumnRules: map[string]TransformerRules{
							"column_1": {
								Name: "string",
							},
						},
					},
					{
						ValidationMode: validationModeRelaxed,
						Schema:         "test",
						Table:          "test2",
						ColumnRules: map[string]TransformerRules{
							"column_2": {
								Name: "string",
							},
						},
					},
				},
			},
			parseFn: func(ctx context.Context, rules Rules) (map[string]ColumnTransformers, error) {
				return testTransformerMap, nil
			},

			wantTransformer: &Transformer{
				transformerMap: testTransformerMap,
				validationMode: validationModeTableLevel,
				tableValidationModes: map[string]string{
					"\"public\".\"test1\"": validationModeStrict,
					"\"test\".\"test2\"":   validationModeRelaxed,
				},
			},
			wantErr: nil,
		},
		{
			name: "error - parsing rules",
			config: &Config{
				ValidationMode: validationModeTableLevel,
				TransformerRules: []TableRules{
					{
						ValidationMode: validationModeStrict,
						Schema:         "public",
						Table:          "test1",
						ColumnRules: map[string]TransformerRules{
							"column_1": {
								Name: "string",
							},
						},
					},
					{
						ValidationMode: validationModeRelaxed,
						Schema:         "test",
						Table:          "test2",
						ColumnRules: map[string]TransformerRules{
							"column_2": {
								Name: "string",
							},
						},
					},
				},
			},
			parseFn: func(ctx context.Context, rules Rules) (map[string]ColumnTransformers, error) {
				return nil, errTest
			},

			wantTransformer: nil,
			wantErr:         errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opts := []Option{}
			if tc.parseFn != nil {
				opts = append(opts, func(t *Transformer) {
					t.parser = tc.parseFn
				})
			}

			transformer, err := New(context.Background(), tc.config, mockProcessor, mockBuilder, opts...)
			require.ErrorIs(t, err, tc.wantErr)

			diff := cmp.Diff(tc.wantTransformer, transformer,
				cmp.AllowUnexported(Transformer{}),
				cmpopts.IgnoreFields(Transformer{}, "parser", "processor", "logger", "walDataToDDLEvent", "ddlEventToSchemaDiff"))
			require.Empty(t, diff)
		})
	}
}

func TestTransformer_ProcessWALEvent(t *testing.T) {
	t.Parallel()

	testSchema := "test_schema"
	testTable := "test_table"
	errTest := errors.New("oh noes")
	testKey := "\"test_schema\".\"test_table\""

	newTestEvent := func(cols []wal.Column) *wal.Event {
		return &wal.Event{
			CommitPosition: "1",
			Data: &wal.Data{
				Action:  "I",
				Schema:  testSchema,
				Table:   testTable,
				Columns: cols,
			},
		}
	}

	tests := []struct {
		name                 string
		event                *wal.Event
		processor            processor.Processor
		transformerMap       map[string]ColumnTransformers
		validationMode       string
		ddlEventToSchemaDiff func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error)

		wantErr error
	}{
		{
			name:  "ok - no data",
			event: &wal.Event{},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, &wal.Event{}, walEvent)
					return nil
				},
			},
			transformerMap: map[string]ColumnTransformers{},

			wantErr: nil,
		},
		{
			name: "ok - no transformation rules",
			event: &wal.Event{
				Data: &wal.Data{},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, &wal.Event{
						Data: &wal.Data{},
					}, walEvent)
					return nil
				},
			},
			transformerMap: map[string]ColumnTransformers{},

			wantErr: nil,
		},
		{
			name: "ok - ddl event",
			event: &wal.Event{
				Data: &wal.Data{
					Action: wal.LogicalMessageAction,
					Prefix: wal.DDLPrefix,
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, &wal.Event{
						Data: &wal.Data{
							Action: wal.LogicalMessageAction,
							Prefix: wal.DDLPrefix,
						},
					}, walEvent)
					return nil
				},
			},
			transformerMap: map[string]ColumnTransformers{},

			wantErr: nil,
		},
		{
			name:  "ok - no transformers for schema table",
			event: newTestEvent(nil),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, newTestEvent(nil), walEvent)
					return nil
				},
			},
			transformerMap: map[string]ColumnTransformers{
				"anotherschema/table": {},
			},

			wantErr: nil,
		},
		{
			name: "ok - with transformers for schema table",
			event: newTestEvent([]wal.Column{
				{Name: "column_1", Type: "text", Value: "one"},
				{Name: "column_2", Type: "int", Value: 1},
			}),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					wantEvent := newTestEvent([]wal.Column{
						{Name: "column_1", Type: "text", Value: "two"},
						{Name: "column_2", Type: "int", Value: 1},
					})
					require.Equal(t, wantEvent, walEvent)
					return nil
				},
			},
			transformerMap: map[string]ColumnTransformers{
				testKey: {
					"column_1": &transformermocks.Transformer{
						TransformFn: func(a transformers.Value) (any, error) {
							require.Nil(t, a.DynamicValues)
							aStr, ok := a.TransformValue.(string)
							require.True(t, ok)
							require.Equal(t, "one", aStr)
							return "two", nil
						},
					},
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - with dynamic transformers for schema table",
			event: newTestEvent([]wal.Column{
				{Name: "column_1", Type: "text", Value: "one"},
				{Name: "column_2", Type: "int", Value: 1},
			}),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					wantEvent := newTestEvent([]wal.Column{
						{Name: "column_1", Type: "text", Value: "two"},
						{Name: "column_2", Type: "int", Value: 1},
					})
					require.Equal(t, wantEvent, walEvent)
					return nil
				},
			},
			transformerMap: map[string]ColumnTransformers{
				testKey: {
					"column_1": &transformermocks.Transformer{
						TransformFn: func(a transformers.Value) (any, error) {
							require.Equal(t, a.DynamicValues, map[string]any{"column_2": 1})
							aStr, ok := a.TransformValue.(string)
							require.True(t, ok)
							require.Equal(t, "one", aStr)
							return "two", nil
						},
						IsDynamicFn: func() bool { return true },
					},
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - nil column value",
			event: newTestEvent([]wal.Column{
				{Name: "column_1", Type: "text", Value: nil},
				{Name: "column_2", Type: "int", Value: 1},
			}),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					wantEvent := newTestEvent([]wal.Column{
						{Name: "column_1", Type: "text", Value: nil},
						{Name: "column_2", Type: "int", Value: 1},
					})
					require.Equal(t, wantEvent, walEvent)
					return nil
				},
			},
			transformerMap: map[string]ColumnTransformers{
				testKey: {
					"column_1": &transformermocks.Transformer{
						TransformFn: func(a transformers.Value) (any, error) {
							return nil, errors.New("TransformFn: should not be called")
						},
					},
				},
			},

			wantErr: nil,
		},
		{
			name: "error - transforming",
			event: newTestEvent([]wal.Column{
				{Name: "column_1", Type: "text", Value: "one"},
				{Name: "column_2", Type: "int", Value: 1},
			}),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					wantEvent := newTestEvent([]wal.Column{
						{Name: "column_1", Type: "text", Value: nil},
						{Name: "column_2", Type: "int", Value: 1},
					})
					require.Equal(t, wantEvent, walEvent)
					return nil
				},
			},
			transformerMap: map[string]ColumnTransformers{
				testKey: {
					"column_1": &transformermocks.Transformer{
						TransformFn: func(a transformers.Value) (any, error) {
							return nil, errTest
						},
					},
				},
			},

			wantErr: nil,
		},
		{
			name: "error - ddl event",
			event: &wal.Event{
				Data: &wal.Data{
					Action: wal.LogicalMessageAction,
					Prefix: wal.DDLPrefix,
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					return errors.New("ProcessWALEvent should not be called")
				},
			},
			transformerMap: map[string]ColumnTransformers{},
			ddlEventToSchemaDiff: func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error) {
				return &wal.SchemaDiff{
					TablesAdded: []wal.DDLObject{
						{Type: "table", Identity: "new_table", Schema: "public"},
					},
				}, nil
			},
			validationMode: validationModeStrict,

			wantErr: errDDLNotSupportedInStrictMode,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformer := &Transformer{
				logger:               log.NewNoopLogger(),
				transformerMap:       tc.transformerMap,
				processor:            tc.processor,
				validationMode:       validationModeRelaxed,
				walDataToDDLEvent:    func(data *wal.Data) (*wal.DDLEvent, error) { return &wal.DDLEvent{}, nil },
				ddlEventToSchemaDiff: tc.ddlEventToSchemaDiff,
			}

			if tc.validationMode != "" {
				transformer.validationMode = tc.validationMode
			}

			err := transformer.ProcessWALEvent(context.Background(), tc.event)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestTransformer_processDDLEvent(t *testing.T) {
	t.Parallel()

	testWALDDLEvent := &wal.Event{
		Data: &wal.Data{
			Action: wal.LogicalMessageAction,
			Prefix: wal.DDLPrefix,
		},
	}

	testDDLEvent := &wal.DDLEvent{
		DDL:        "ALTER TABLE public.test_table ADD COLUMN new_column text",
		SchemaName: "test_schema",
		CommandTag: "ALTER TABLE",
		Objects: []wal.DDLObject{
			{
				Type:     "table",
				Identity: "public.test_table",
			},
		},
	}

	validWalDataToDDLEvent := func(data *wal.Data) (*wal.DDLEvent, error) {
		require.Equal(t, testWALDDLEvent.Data, data)
		return testDDLEvent, nil
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name                 string
		validationMode       string
		tableValidationModes map[string]string
		transformerMap       map[string]ColumnTransformers
		event                *wal.Event
		walDataToDDLEvent    func(data *wal.Data) (*wal.DDLEvent, error)
		ddlEventToSchemaDiff func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error)

		wantErr error
	}{
		{
			name:                 "ok - non-DDL event",
			validationMode:       validationModeStrict,
			tableValidationModes: map[string]string{},
			event: &wal.Event{
				Data: &wal.Data{
					Action: "I",
					Schema: "public",
					Table:  "users",
				},
			},
			walDataToDDLEvent:    nil,
			ddlEventToSchemaDiff: nil,

			wantErr: nil,
		},
		{
			name:                 "ok - DDL event with non-strict mode",
			validationMode:       validationModeRelaxed,
			tableValidationModes: map[string]string{},
			event:                testWALDDLEvent,
			walDataToDDLEvent:    nil,
			ddlEventToSchemaDiff: nil,

			wantErr: nil,
		},
		{
			name:                 "ok - DDL event with empty schema diff",
			validationMode:       validationModeStrict,
			tableValidationModes: map[string]string{},
			event:                testWALDDLEvent,
			walDataToDDLEvent:    validWalDataToDDLEvent,
			ddlEventToSchemaDiff: func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error) {
				require.Equal(t, testDDLEvent, ddlEvent)
				return &wal.SchemaDiff{}, nil
			},

			wantErr: nil,
		},
		{
			name:                 "ok - DDL event with only columns removed",
			validationMode:       validationModeStrict,
			tableValidationModes: map[string]string{},
			event:                testWALDDLEvent,
			walDataToDDLEvent:    validWalDataToDDLEvent,
			ddlEventToSchemaDiff: func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error) {
				require.Equal(t, testDDLEvent, ddlEvent)
				return &wal.SchemaDiff{
					TablesChanged: []wal.TableDiff{
						{
							ColumnsRemoved: []wal.DDLColumn{
								{Name: "old_column", Type: "text"},
							},
						},
					},
				}, nil
			},

			wantErr: nil,
		},
		{
			name:                 "ok - DDL event with columns added with table level validation mode set to relaxed",
			validationMode:       validationModeTableLevel,
			tableValidationModes: map[string]string{`"test_schema"."test_table"`: validationModeRelaxed},
			event:                testWALDDLEvent,
			walDataToDDLEvent:    validWalDataToDDLEvent,
			ddlEventToSchemaDiff: func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error) {
				return &wal.SchemaDiff{
					SchemaName: "test_schema",
					TablesChanged: []wal.TableDiff{
						{
							TableName: "test_table",
							ColumnsAdded: []wal.DDLColumn{
								{Name: "new_column", Type: "text"},
							},
						},
					},
				}, nil
			},

			wantErr: nil,
		},
		{
			name:                 "ok - DDL event with column type changed with table level validation mode set to strict",
			validationMode:       validationModeTableLevel,
			tableValidationModes: map[string]string{`"test_schema"."test_table"`: validationModeStrict},
			transformerMap: map[string]ColumnTransformers{
				`"test_schema"."test_table"`: {},
			},
			event:             testWALDDLEvent,
			walDataToDDLEvent: validWalDataToDDLEvent,
			ddlEventToSchemaDiff: func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error) {
				return &wal.SchemaDiff{
					SchemaName: "test_schema",
					TablesChanged: []wal.TableDiff{
						{
							TableName: "test_table",
							ColumnsChanged: []wal.ColumnDiff{
								{
									ColumnName: "test_column",
									TypeChange: &wal.ValueChange[string]{Old: "text", New: "int"},
								},
							},
						},
					},
				}, nil
			},

			wantErr: nil,
		},
		{
			name:                 "ok - DDL event with table rename and table exists in transformation rules",
			validationMode:       validationModeStrict,
			tableValidationModes: map[string]string{},
			transformerMap: map[string]ColumnTransformers{
				`"test_schema"."old_table"`: {},
				`"test_schema"."new_table"`: {},
			},
			event:             testWALDDLEvent,
			walDataToDDLEvent: validWalDataToDDLEvent,
			ddlEventToSchemaDiff: func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error) {
				return &wal.SchemaDiff{
					SchemaName: "test_schema",
					TablesChanged: []wal.TableDiff{
						{
							TableName: "new_table",
							TableNameChange: &wal.ValueChange[string]{
								Old: "old_table",
								New: "new_table",
							},
						},
					},
				}, nil
			},

			wantErr: nil,
		},
		{
			name:                 "ok - DDL event with column rename and column exists in transformation rules",
			validationMode:       validationModeStrict,
			tableValidationModes: map[string]string{},
			transformerMap: map[string]ColumnTransformers{
				`"test_schema"."test_table"`: {
					"old_column": nil,
					"new_column": nil,
				},
			},
			event:             testWALDDLEvent,
			walDataToDDLEvent: validWalDataToDDLEvent,
			ddlEventToSchemaDiff: func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error) {
				return &wal.SchemaDiff{
					SchemaName: "test_schema",
					TablesChanged: []wal.TableDiff{
						{
							TableName: "test_table",
							ColumnsChanged: []wal.ColumnDiff{
								{
									ColumnName: "new_column",
									NameChange: &wal.ValueChange[string]{
										Old: "old_column",
										New: "new_column",
									},
								},
							},
						},
					},
				}, nil
			},

			wantErr: nil,
		},
		{
			name:                 "error - DDL event with table rename and new table not in transformation rules",
			validationMode:       validationModeStrict,
			tableValidationModes: map[string]string{},
			event:                testWALDDLEvent,
			walDataToDDLEvent:    validWalDataToDDLEvent,
			ddlEventToSchemaDiff: func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error) {
				return &wal.SchemaDiff{
					SchemaName: "test_schema",
					TablesChanged: []wal.TableDiff{
						{
							TableName: "missing_table",
							TableNameChange: &wal.ValueChange[string]{
								Old: "old_table",
								New: "missing_table",
							},
						},
					},
				}, nil
			},

			wantErr: errDDLNotSupportedInStrictMode,
		},
		{
			name:                 "error - DDL event with column rename and new column not in transformation rules",
			validationMode:       validationModeStrict,
			tableValidationModes: map[string]string{},
			event:                testWALDDLEvent,
			walDataToDDLEvent:    validWalDataToDDLEvent,
			ddlEventToSchemaDiff: func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error) {
				return &wal.SchemaDiff{
					SchemaName: "test_schema",
					TablesChanged: []wal.TableDiff{
						{
							TableName: "test_table",
							ColumnsChanged: []wal.ColumnDiff{
								{
									ColumnName: "missing_column",
									NameChange: &wal.ValueChange[string]{
										Old: "old_column",
										New: "missing_column",
									},
								},
							},
						},
					},
				}, nil
			},

			wantErr: errDDLNotSupportedInStrictMode,
		},
		{
			name:                 "ok - DDL event with multiple changes including table rename with relaxed validation",
			validationMode:       validationModeTableLevel,
			tableValidationModes: map[string]string{`"test_schema"."new_table"`: validationModeRelaxed},
			transformerMap: map[string]ColumnTransformers{
				`"test_schema"."old_table"`: {},
				`"test_schema"."new_table"`: {},
			},
			event:             testWALDDLEvent,
			walDataToDDLEvent: validWalDataToDDLEvent,
			ddlEventToSchemaDiff: func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error) {
				return &wal.SchemaDiff{
					SchemaName: "test_schema",
					TablesChanged: []wal.TableDiff{
						{
							TableName: "new_table",
							TableNameChange: &wal.ValueChange[string]{
								Old: "old_table",
								New: "new_table",
							},
							ColumnsAdded: []wal.DDLColumn{
								{Name: "new_column", Type: "text"},
							},
							ColumnsChanged: []wal.ColumnDiff{
								{
									ColumnName: "renamed_column",
									NameChange: &wal.ValueChange[string]{
										Old: "old_column",
										New: "renamed_column",
									},
								},
							},
						},
					},
				}, nil
			},

			wantErr: nil,
		},
		{
			name:                 "error - DDL event with multiple changes in strict mode missing transformation rules",
			validationMode:       validationModeStrict,
			tableValidationModes: map[string]string{},
			transformerMap: map[string]ColumnTransformers{
				`"test_schema"."test_table"`: {
					"old_column": nil,
				},
			},
			event:             testWALDDLEvent,
			walDataToDDLEvent: validWalDataToDDLEvent,
			ddlEventToSchemaDiff: func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error) {
				return &wal.SchemaDiff{
					SchemaName: "test_schema",
					TablesChanged: []wal.TableDiff{
						{
							TableName: "renamed_table",
							TableNameChange: &wal.ValueChange[string]{
								Old: "test_table",
								New: "renamed_table",
							},
							ColumnsAdded: []wal.DDLColumn{
								{Name: "new_column", Type: "text"},
							},
							ColumnsChanged: []wal.ColumnDiff{
								{
									ColumnName: "renamed_column",
									NameChange: &wal.ValueChange[string]{
										Old: "old_column",
										New: "renamed_column",
									},
								},
							},
						},
					},
				}, nil
			},

			wantErr: errDDLNotSupportedInStrictMode,
		},
		{
			name:                 "error - DDL event with columns added with table level validation mode set to strict",
			validationMode:       validationModeTableLevel,
			tableValidationModes: map[string]string{`"test_schema"."test_table"`: validationModeStrict},
			event:                testWALDDLEvent,
			walDataToDDLEvent:    validWalDataToDDLEvent,
			ddlEventToSchemaDiff: func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error) {
				return &wal.SchemaDiff{
					SchemaName: "test_schema",
					TablesChanged: []wal.TableDiff{
						{
							TableName: "test_table",
							ColumnsAdded: []wal.DDLColumn{
								{Name: "new_column", Type: "text"},
							},
						},
					},
				}, nil
			},

			wantErr: errDDLNotSupportedInStrictMode,
		},
		{
			name:                 "error - DDL event with tables added",
			validationMode:       validationModeStrict,
			tableValidationModes: map[string]string{},
			event:                testWALDDLEvent,
			walDataToDDLEvent:    validWalDataToDDLEvent,
			ddlEventToSchemaDiff: func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error) {
				require.Equal(t, testDDLEvent, ddlEvent)
				return &wal.SchemaDiff{
					TablesAdded: []wal.DDLObject{
						{Type: "table", Identity: "new_table", Schema: "public"},
					},
				}, nil
			},

			wantErr: errDDLNotSupportedInStrictMode,
		},
		{
			name:                 "error - DDL event with columns added",
			validationMode:       validationModeStrict,
			tableValidationModes: map[string]string{},
			event:                testWALDDLEvent,
			walDataToDDLEvent:    validWalDataToDDLEvent,
			ddlEventToSchemaDiff: func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error) {
				return &wal.SchemaDiff{
					TablesChanged: []wal.TableDiff{
						{
							ColumnsAdded: []wal.DDLColumn{
								{Name: "new_column", Type: "text"},
							},
						},
					},
				}, nil
			},

			wantErr: errDDLNotSupportedInStrictMode,
		},
		{
			name:                 "error - walDataToDDLEvent fails",
			validationMode:       validationModeStrict,
			tableValidationModes: map[string]string{},
			event:                testWALDDLEvent,
			walDataToDDLEvent: func(data *wal.Data) (*wal.DDLEvent, error) {
				return nil, errTest
			},
			ddlEventToSchemaDiff: nil,

			wantErr: errTest,
		},
		{
			name:                 "error - ddlEventToSchemaDiff fails",
			validationMode:       validationModeStrict,
			tableValidationModes: map[string]string{},
			event:                testWALDDLEvent,
			walDataToDDLEvent:    validWalDataToDDLEvent,
			ddlEventToSchemaDiff: func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error) {
				return nil, errTest
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformer := &Transformer{
				logger:               log.NewNoopLogger(),
				tableValidationModes: tc.tableValidationModes,
				validationMode:       tc.validationMode,
				walDataToDDLEvent:    tc.walDataToDDLEvent,
				ddlEventToSchemaDiff: tc.ddlEventToSchemaDiff,
				transformerMap:       tc.transformerMap,
			}

			err := transformer.processDDLEvent(tc.event)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestTransformer_validateTableDDL(t *testing.T) {
	t.Parallel()

	testSchema := "test_schema"
	testTable := "test_table"
	testDDL := "ALTER TABLE test_schema.test_table ADD COLUMN new_column text"
	testColumns := []wal.DDLColumn{
		{Name: "new_column", Type: "text"},
		{Name: "another_column", Type: "int"},
	}

	testTransformer := &transformermocks.Transformer{}

	tests := []struct {
		name                 string
		validationMode       string
		tableValidationModes map[string]string
		transformerMap       map[string]ColumnTransformers
		schema               string
		table                string
		ddl                  string
		columns              []wal.DDLColumn

		wantErr error
	}{
		{
			name:           "ok - empty columns slice",
			validationMode: validationModeStrict,
			transformerMap: map[string]ColumnTransformers{
				`"test_schema"."test_table"`: {},
			},
			schema:  testSchema,
			table:   testTable,
			ddl:     testDDL,
			columns: []wal.DDLColumn{},

			wantErr: nil,
		},
		{
			name:                 "ok - relaxed validation mode",
			validationMode:       validationModeRelaxed,
			tableValidationModes: map[string]string{},
			transformerMap:       map[string]ColumnTransformers{},
			schema:               testSchema,
			table:                testTable,
			ddl:                  testDDL,
			columns:              testColumns,

			wantErr: nil,
		},
		{
			name:                 "ok - table level validation mode set to relaxed",
			validationMode:       validationModeTableLevel,
			tableValidationModes: map[string]string{`"test_schema"."test_table"`: validationModeRelaxed},
			transformerMap:       map[string]ColumnTransformers{},
			schema:               testSchema,
			table:                testTable,
			ddl:                  testDDL,
			columns:              testColumns,

			wantErr: nil,
		},
		{
			name:           "ok - strict validation mode with all columns in transformation rules",
			validationMode: validationModeStrict,
			transformerMap: map[string]ColumnTransformers{
				`"test_schema"."test_table"`: {
					"new_column":     testTransformer,
					"another_column": testTransformer,
				},
			},
			schema:  testSchema,
			table:   testTable,
			ddl:     testDDL,
			columns: testColumns,

			wantErr: nil,
		},
		{
			name:                 "ok - table level validation mode set to strict with all columns in transformation rules",
			validationMode:       validationModeTableLevel,
			tableValidationModes: map[string]string{`"test_schema"."test_table"`: validationModeStrict},
			transformerMap: map[string]ColumnTransformers{
				`"test_schema"."test_table"`: {
					"new_column":     testTransformer,
					"another_column": testTransformer,
				},
			},
			schema:  testSchema,
			table:   testTable,
			ddl:     testDDL,
			columns: testColumns,

			wantErr: nil,
		},
		{
			name:           "error - strict validation mode with table not in transformation rules",
			validationMode: validationModeStrict,
			transformerMap: map[string]ColumnTransformers{
				`"other_schema"."other_table"`: {
					"column": testTransformer,
				},
			},
			schema:  testSchema,
			table:   testTable,
			ddl:     testDDL,
			columns: testColumns,

			wantErr: errDDLNotSupportedInStrictMode,
		},
		{
			name:                 "error - table level validation mode set to strict with table not in transformation rules",
			validationMode:       validationModeTableLevel,
			tableValidationModes: map[string]string{`"test_schema"."test_table"`: validationModeStrict},
			transformerMap:       map[string]ColumnTransformers{},
			schema:               testSchema,
			table:                testTable,
			ddl:                  testDDL,
			columns:              testColumns,

			wantErr: errDDLNotSupportedInStrictMode,
		},
		{
			name:           "error - strict validation mode with column not in transformation rules",
			validationMode: validationModeStrict,
			transformerMap: map[string]ColumnTransformers{
				`"test_schema"."test_table"`: {
					"new_column": testTransformer,
					// missing "another_column"
				},
			},
			schema:  testSchema,
			table:   testTable,
			ddl:     testDDL,
			columns: testColumns,

			wantErr: errDDLNotSupportedInStrictMode,
		},
		{
			name:                 "error - table level validation mode defaults to strict when table not found",
			validationMode:       validationModeTableLevel,
			tableValidationModes: map[string]string{}, // table not found, defaults to strict
			transformerMap:       map[string]ColumnTransformers{},
			schema:               testSchema,
			table:                testTable,
			ddl:                  testDDL,
			columns:              testColumns,

			wantErr: errDDLNotSupportedInStrictMode,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformer := &Transformer{
				logger:               log.NewNoopLogger(),
				validationMode:       tc.validationMode,
				tableValidationModes: tc.tableValidationModes,
				transformerMap:       tc.transformerMap,
			}

			err := transformer.validateTableDDL(tc.schema, tc.table, tc.ddl, tc.columns)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
