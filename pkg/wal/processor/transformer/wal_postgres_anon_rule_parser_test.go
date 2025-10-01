// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/transformers"
	transformermocks "github.com/xataio/pgstream/pkg/transformers/mocks"
	"gopkg.in/yaml.v3"
)

func TestAnonRuleParser_ParseAndValidate(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	validTransformers := map[string]ColumnTransformers{
		"public.users": map[string]transformers.Transformer{
			"email": &transformermocks.Transformer{},
		},
	}

	const anonPartialEmailFn = "anon.partial_email"
	testRows := func() *pgmocks.Rows {
		return &pgmocks.Rows{
			NextFn: func(i uint) bool { return i <= 1 },
			ScanFn: func(i uint, dest ...any) error {
				require.Len(t, dest, 4)
				schema, ok := dest[0].(*string)
				require.True(t, ok)
				*schema = "public"
				table, ok := dest[1].(*string)
				require.True(t, ok)
				*table = "users"
				column, ok := dest[2].(*string)
				require.True(t, ok)
				*column = "email"
				function, ok := dest[3].(*string)
				require.True(t, ok)
				*function = anonPartialEmailFn
				return nil
			},
			ErrFn: func() error { return nil },
		}
	}

	tests := []struct {
		name       string
		conn       *pgmocks.Querier
		parser     ParseFn
		dumpToFile bool
		marshaler  func(v any) ([]byte, error)

		wantFile   bool
		wantResult map[string]ColumnTransformers
		wantErr    error
	}{
		{
			name: "ok with anon rules parsed and validated",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return testRows(), nil
				},
			},
			parser: func(ctx context.Context, rules Rules) (map[string]ColumnTransformers, error) {
				return validTransformers, nil
			},
			dumpToFile: false,
			wantResult: validTransformers,
			wantErr:    nil,
		},
		{
			name: "ok with dump to file enabled",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return testRows(), nil
				},
			},
			parser: func(ctx context.Context, rules Rules) (map[string]ColumnTransformers, error) {
				return validTransformers, nil
			},
			dumpToFile: true,
			wantFile:   true,
			wantResult: validTransformers,
			wantErr:    nil,
		},
		{
			name: "ok with no anon rules found",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return &pgmocks.Rows{
						NextFn: func(i uint) bool { return false },
						ScanFn: func(i uint, dest ...any) error { return nil },
						ErrFn:  func() error { return nil },
					}, nil
				},
			},
			parser: func(ctx context.Context, rules Rules) (map[string]ColumnTransformers, error) {
				return map[string]ColumnTransformers{}, nil
			},
			dumpToFile: false,
			wantResult: map[string]ColumnTransformers{},
			wantErr:    nil,
		},
		{
			name: "error getting anon masking rules",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return nil, errTest
				},
			},
			parser: func(ctx context.Context, rules Rules) (map[string]ColumnTransformers, error) {
				return nil, nil
			},
			dumpToFile: false,
			wantResult: nil,
			wantErr:    errTest,
		},
		{
			name: "error in parser validation",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return testRows(), nil
				},
			},
			parser: func(ctx context.Context, rules Rules) (map[string]ColumnTransformers, error) {
				return nil, errTest
			},
			dumpToFile: false,
			wantResult: nil,
			wantErr:    errTest,
		},
		{
			name: "ok with dump to file enabled and error dumping to file",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return testRows(), nil
				},
			},
			parser: func(ctx context.Context, rules Rules) (map[string]ColumnTransformers, error) {
				return validTransformers, nil
			},
			dumpToFile: true,
			wantFile:   false,
			marshaler:  func(v any) ([]byte, error) { return nil, errTest },
			wantResult: validTransformers,
			wantErr:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Clean up any test files
			defer func() {
				if tc.dumpToFile {
					_ = os.Remove(anonRulesFile)
				}
			}()

			p := &AnonRuleParser{
				conn:       tc.conn,
				logger:     loglib.NewNoopLogger(),
				parser:     tc.parser,
				dumpToFile: tc.dumpToFile,
				marshaler:  yaml.Marshal,
			}

			if tc.marshaler != nil {
				p.marshaler = tc.marshaler
			}

			result, err := p.ParseAndValidate(context.Background(), Rules{
				Transformers:   nil,
				ValidationMode: "relaxed",
			})
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantResult, result)

			// Verify file was created
			if tc.wantFile {
				_, err := os.Stat(anonRulesFile)
				require.NoError(t, err, "expected rules file to be created")
			}
		})
	}
}

func TestAnonRuleParser_Close(t *testing.T) {
	t.Parallel()

	errTest := errors.New("close error")

	tests := []struct {
		name    string
		conn    *pgmocks.Querier
		wantErr error
	}{
		{
			name: "successful close",
			conn: &pgmocks.Querier{
				CloseFn: func(ctx context.Context) error {
					return nil
				},
			},
			wantErr: nil,
		},
		{
			name: "error on close",
			conn: &pgmocks.Querier{
				CloseFn: func(ctx context.Context) error {
					return errTest
				},
			},
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := &AnonRuleParser{
				conn: tc.conn,
			}

			err := p.Close()
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestAnonRuleParser_parseAnonMaskingRules(t *testing.T) {
	t.Parallel()

	anonRandomHashFn := "anon.random_hash"
	anonPartialEmailFn := "anon.partial_email"
	errTest := errors.New("oh noes")

	tests := []struct {
		name string
		conn *pgmocks.Querier

		wantRules []TableRules
		wantErr   error
	}{
		{
			name: "ok with multiple rules for same table",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return &pgmocks.Rows{
						NextFn: func(i uint) bool { return i <= 2 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 4)
							switch i {
							case 1:
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								*schema = "public"
								table, ok := dest[1].(*string)
								require.True(t, ok)
								*table = "users"
								column, ok := dest[2].(*string)
								require.True(t, ok)
								*column = "email"
								function, ok := dest[3].(*string)
								require.True(t, ok)
								*function = "anon.partial_email(users.email)"
							case 2:
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								*schema = "public"
								table, ok := dest[1].(*string)
								require.True(t, ok)
								*table = "users"
								column, ok := dest[2].(*string)
								require.True(t, ok)
								*column = "name"
								function, ok := dest[3].(*string)
								require.True(t, ok)
								*function = anonRandomHashFn
							default:
								return errors.New("unexpected call to ScanFn")
							}
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},

			wantRules: []TableRules{
				{
					Schema: "public",
					Table:  "users",
					ColumnRules: map[string]TransformerRules{
						"email": {
							Name: string(transformers.PGAnonymizer),
							Parameters: map[string]any{
								"anon_function": anonPartialEmailFn,
							},
						},
						"name": {
							Name: string(transformers.PGAnonymizer),
							Parameters: map[string]any{
								"anon_function": anonRandomHashFn,
							},
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ok with rules for different tables",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return &pgmocks.Rows{
						NextFn: func(i uint) bool { return i <= 2 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 4)
							switch i {
							case 1:
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								*schema = "public"
								table, ok := dest[1].(*string)
								require.True(t, ok)
								*table = "users"
								column, ok := dest[2].(*string)
								require.True(t, ok)
								*column = "email"
								function, ok := dest[3].(*string)
								require.True(t, ok)
								*function = anonPartialEmailFn
							case 2:
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								*schema = "public"
								table, ok := dest[1].(*string)
								require.True(t, ok)
								*table = "orders"
								column, ok := dest[2].(*string)
								require.True(t, ok)
								*column = "amount"
								function, ok := dest[3].(*string)
								require.True(t, ok)
								*function = "anon.noise(value, 0.1)"
							default:
								return errors.New("unexpected call to ScanFn")
							}
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},

			wantRules: []TableRules{
				{
					Schema: "public",
					Table:  "users",
					ColumnRules: map[string]TransformerRules{
						"email": {
							Name: string(transformers.PGAnonymizer),
							Parameters: map[string]any{
								"anon_function": anonPartialEmailFn,
							},
						},
					},
				},
				{
					Schema: "public",
					Table:  "orders",
					ColumnRules: map[string]TransformerRules{
						"amount": {
							Name: string(transformers.PGAnonymizer),
							Parameters: map[string]any{
								"anon_function": "anon.noise",
								"ratio":         0.1,
							},
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ok no rules",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return &pgmocks.Rows{
						NextFn: func(i uint) bool { return false },
						ScanFn: func(i uint, dest ...any) error { return nil },
						ErrFn:  func() error { return nil },
					}, nil
				},
			},

			wantRules: nil,
			wantErr:   nil,
		},
		{
			name: "skip invalid rule with empty schema",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return &pgmocks.Rows{
						NextFn: func(i uint) bool { return i <= 2 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 4)
							switch i {
							case 1:
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								*schema = ""
								table, ok := dest[1].(*string)
								require.True(t, ok)
								*table = "users"
								column, ok := dest[2].(*string)
								require.True(t, ok)
								*column = "email"
								function, ok := dest[3].(*string)
								require.True(t, ok)
								*function = anonPartialEmailFn
							case 2:
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								*schema = "public"
								table, ok := dest[1].(*string)
								require.True(t, ok)
								*table = "users"
								column, ok := dest[2].(*string)
								require.True(t, ok)
								*column = "name"
								function, ok := dest[3].(*string)
								require.True(t, ok)
								*function = anonRandomHashFn
							default:
								return errors.New("unexpected call to ScanFn")
							}
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},

			wantRules: []TableRules{
				{
					Schema: "public",
					Table:  "users",
					ColumnRules: map[string]TransformerRules{
						"name": {
							Name: string(transformers.PGAnonymizer),
							Parameters: map[string]any{
								"anon_function": anonRandomHashFn,
							},
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "skip invalid rule with empty function",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return &pgmocks.Rows{
						NextFn: func(i uint) bool { return i <= 2 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 4)
							switch i {
							case 1:
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								*schema = "public"
								table, ok := dest[1].(*string)
								require.True(t, ok)
								*table = "users"
								column, ok := dest[2].(*string)
								require.True(t, ok)
								*column = "email"
								function, ok := dest[3].(*string)
								require.True(t, ok)
								*function = ""
							case 2:
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								*schema = "public"
								table, ok := dest[1].(*string)
								require.True(t, ok)
								*table = "users"
								column, ok := dest[2].(*string)
								require.True(t, ok)
								*column = "name"
								function, ok := dest[3].(*string)
								require.True(t, ok)
								*function = anonRandomHashFn
							default:
								return errors.New("unexpected call to ScanFn")
							}
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},

			wantRules: []TableRules{
				{
					Schema: "public",
					Table:  "users",
					ColumnRules: map[string]TransformerRules{
						"name": {
							Name: string(transformers.PGAnonymizer),
							Parameters: map[string]any{
								"anon_function": anonRandomHashFn,
							},
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "skip rule with invalid anon function",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return &pgmocks.Rows{
						NextFn: func(i uint) bool { return i <= 2 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 4)
							switch i {
							case 1:
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								*schema = "public"
								table, ok := dest[1].(*string)
								require.True(t, ok)
								*table = "users"
								column, ok := dest[2].(*string)
								require.True(t, ok)
								*column = "email"
								function, ok := dest[3].(*string)
								require.True(t, ok)
								*function = "anon.digest(value, 'salt')"
							case 2:
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								*schema = "public"
								table, ok := dest[1].(*string)
								require.True(t, ok)
								*table = "users"
								column, ok := dest[2].(*string)
								require.True(t, ok)
								*column = "name"
								function, ok := dest[3].(*string)
								require.True(t, ok)
								*function = anonRandomHashFn
							default:
								return errors.New("unexpected call to ScanFn")
							}
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},

			wantRules: []TableRules{
				{
					Schema: "public",
					Table:  "users",
					ColumnRules: map[string]TransformerRules{
						"name": {
							Name: string(transformers.PGAnonymizer),
							Parameters: map[string]any{
								"anon_function": anonRandomHashFn,
							},
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "error getting masking rules",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return nil, errTest
				},
			},

			wantRules: nil,
			wantErr:   errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := AnonRuleParser{
				conn:   tc.conn,
				logger: loglib.NewNoopLogger(),
			}

			rules, err := p.parseAnonMaskingRules(context.Background())
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantRules, rules)
		})
	}
}

func TestAnonRuleParser_getMaskingRules(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name string
		conn *pgmocks.Querier

		wantRules []maskingRule
		wantErr   error
	}{
		{
			name: "ok with multiple rules",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return &pgmocks.Rows{
						NextFn: func(i uint) bool { return i <= 2 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 4)
							switch i {
							case 1:
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								*schema = "public"
								table, ok := dest[1].(*string)
								require.True(t, ok)
								*table = "users"
								column, ok := dest[2].(*string)
								require.True(t, ok)
								*column = "email"
								function, ok := dest[3].(*string)
								require.True(t, ok)
								*function = "anon.partial_email(users.email)"
							case 2:
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								*schema = "public"
								table, ok := dest[1].(*string)
								require.True(t, ok)
								*table = "users"
								column, ok := dest[2].(*string)
								require.True(t, ok)
								*column = "name"
								function, ok := dest[3].(*string)
								require.True(t, ok)
								*function = "anon.fake_first_name()"
							default:
								return errors.New("unexpected call to ScanFn")
							}
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},

			wantRules: []maskingRule{
				{
					schema:   "public",
					table:    "users",
					column:   "email",
					function: "anon.partial_email(users.email)",
				},
				{
					schema:   "public",
					table:    "users",
					column:   "name",
					function: "anon.fake_first_name()",
				},
			},
			wantErr: nil,
		},
		{
			name: "ok no rules",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return &pgmocks.Rows{
						NextFn: func(i uint) bool { return false },
						ScanFn: func(i uint, dest ...any) error { return nil },
						ErrFn:  func() error { return nil },
					}, nil
				},
			},

			wantRules: []maskingRule{},
			wantErr:   nil,
		},
		{
			name: "error querying rules",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return nil, errTest
				},
			},

			wantRules: nil,
			wantErr:   errTest,
		},
		{
			name: "error scanning rules",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return &pgmocks.Rows{
						NextFn: func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error { return errTest },
						ErrFn:  func() error { return nil },
					}, nil
				},
			},

			wantRules: nil,
			wantErr:   errTest,
		},
		{
			name: "error in rows",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, sql string, args ...any) (pglib.Rows, error) {
					return &pgmocks.Rows{
						NextFn: func(i uint) bool { return false },
						ScanFn: func(i uint, dest ...any) error { return nil },
						ErrFn:  func() error { return errTest },
					}, nil
				},
			},

			wantRules: nil,
			wantErr:   errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := AnonRuleParser{
				conn: tc.conn,
			}

			rules, err := p.getMaskingRules(context.Background())
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantRules, rules)
		})
	}
}

func TestAnonRuleParser_anonMaskingToTransformerRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		anonRule maskingRule

		wantRule TransformerRules
		wantErr  error
	}{
		{
			name: "random_hash function",
			anonRule: maskingRule{
				schema:   "public",
				table:    "users",
				column:   "email",
				function: "anon.random_hash",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.random_hash",
				},
			},
			wantErr: nil,
		},
		{
			name: "hash function",
			anonRule: maskingRule{
				schema:   "public",
				table:    "users",
				column:   "password",
				function: "anon.hash",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.hash",
				},
			},
			wantErr: nil,
		},
		{
			name: "partial_email function",
			anonRule: maskingRule{
				schema:   "public",
				table:    "users",
				column:   "email",
				function: "anon.partial_email",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.partial_email",
				},
			},
			wantErr: nil,
		},
		{
			name: "digest function with parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "users",
				column:   "ssn",
				function: "anon.digest(value, 'salt123', 'sha256')",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.digest",
					"salt":          "salt123",
					"algorithm":     "sha256",
				},
			},
			wantErr: nil,
		},
		{
			name: "digest function with insufficient parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "users",
				column:   "ssn",
				function: "anon.digest(value, 'salt123')",
			},
			wantRule: TransformerRules{},
			wantErr:  errMissingParameters,
		},
		{
			name: "noise function with ratio",
			anonRule: maskingRule{
				schema:   "public",
				table:    "sales",
				column:   "amount",
				function: "anon.noise(value, 0.1)",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.noise",
					"ratio":         0.1,
				},
			},
			wantErr: nil,
		},
		{
			name: "noise function with insufficient parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "sales",
				column:   "amount",
				function: "anon.noise(value)",
			},
			wantRule: TransformerRules{},
			wantErr:  errMissingParameters,
		},
		{
			name: "dnoise function with interval",
			anonRule: maskingRule{
				schema:   "public",
				table:    "events",
				column:   "timestamp",
				function: "anon.dnoise(value, '1 day')",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.dnoise",
					"interval":      "1 day",
				},
			},
			wantErr: nil,
		},
		{
			name: "image_blur function with sigma",
			anonRule: maskingRule{
				schema:   "public",
				table:    "photos",
				column:   "image_data",
				function: "anon.image_blur(data, 2.5)",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.image_blur",
					"sigma":         2.5,
				},
			},
			wantErr: nil,
		},
		{
			name: "partial function with all parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "users",
				column:   "phone",
				function: "anon.partial(value, 3, 'X', 2)",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function":     "anon.partial",
					"mask_prefix_count": 3,
					"mask":              "X",
					"mask_suffix_count": 2,
				},
			},
			wantErr: nil,
		},
		{
			name: "partial function with insufficient parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "users",
				column:   "phone",
				function: "anon.partial(value, 3, 'X')",
			},
			wantRule: TransformerRules{},
			wantErr:  errMissingParameters,
		},
		{
			name: "random_in_int4range function",
			anonRule: maskingRule{
				schema:   "public",
				table:    "products",
				column:   "price_range",
				function: "anon.random_in_int4range('[1,100]')",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.random_in_int4range",
					"range":         "[1,100]",
				},
			},
			wantErr: nil,
		},
		{
			name: "random_in array function",
			anonRule: maskingRule{
				schema:   "public",
				table:    "products",
				column:   "color",
				function: "anon.random_in(ARRAY['red', 'green', 'blue'])",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.random_in",
					"range":         "ARRAY['red', 'green', 'blue']",
				},
			},
			wantErr: nil,
		},
		{
			name: "random_in array function missing parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "products",
				column:   "color",
				function: "anon.random_in()",
			},
			wantRule: TransformerRules{},
			wantErr:  errMissingParameters,
		},
		{
			name: "random_string function with length",
			anonRule: maskingRule{
				schema:   "public",
				table:    "tokens",
				column:   "token",
				function: "anon.random_string(10)",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.random_string",
					"count":         10,
				},
			},
			wantErr: nil,
		},
		{
			name: "random_phone function with prefix",
			anonRule: maskingRule{
				schema:   "public",
				table:    "contacts",
				column:   "phone",
				function: "anon.random_phone('+1')",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.random_phone",
					"prefix":        "+1",
				},
			},
			wantErr: nil,
		},
		{
			name: "random_phone function without prefix",
			anonRule: maskingRule{
				schema:   "public",
				table:    "contacts",
				column:   "phone",
				function: "anon.random_phone()",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.random_phone",
				},
			},
			wantErr: nil,
		},
		{
			name: "random_int_between function",
			anonRule: maskingRule{
				schema:   "public",
				table:    "ages",
				column:   "age",
				function: "anon.random_int_between(18, 65)",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.random_int_between",
					"min":           "18",
					"max":           "65",
				},
			},
			wantErr: nil,
		},
		{
			name: "lorem_ipsum function without parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "articles",
				column:   "content",
				function: "anon.lorem_ipsum",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.lorem_ipsum",
				},
			},
			wantErr: nil,
		},
		{
			name: "lorem_ipsum function with count",
			anonRule: maskingRule{
				schema:   "public",
				table:    "articles",
				column:   "summary",
				function: "anon.lorem_ipsum(5)",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.lorem_ipsum",
					"count":         5,
				},
			},
			wantErr: nil,
		},
		{
			name: "lorem_ipsum function with unit and count",
			anonRule: maskingRule{
				schema:   "public",
				table:    "articles",
				column:   "body",
				function: "anon.lorem_ipsum(words := 100)",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.lorem_ipsum",
					"unit":          "words",
					"count":         100,
				},
			},
			wantErr: nil,
		},
		{
			name: "pseudo function without salt",
			anonRule: maskingRule{
				schema:   "public",
				table:    "users",
				column:   "first_name",
				function: "anon.pseudo_first_name",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.pseudo_first_name",
					"salt":          "",
				},
			},
			wantErr: nil,
		},
		{
			name: "pseudo function with salt",
			anonRule: maskingRule{
				schema:   "public",
				table:    "users",
				column:   "last_name",
				function: "anon.pseudo_last_name(value, 'mysalt')",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.pseudo_last_name",
					"salt":          "mysalt",
				},
			},
			wantErr: nil,
		},
		{
			name: "dummy locale function without locale",
			anonRule: maskingRule{
				schema:   "public",
				table:    "addresses",
				column:   "city",
				function: "anon.dummy_city_locale",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.dummy_city_locale",
					"locale":        "",
				},
			},
			wantErr: nil,
		},
		{
			name: "dummy locale function with locale",
			anonRule: maskingRule{
				schema:   "public",
				table:    "addresses",
				column:   "country",
				function: "anon.dummy_country_locale('fr_FR')",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.dummy_country_locale",
					"locale":        "fr_FR",
				},
			},
			wantErr: nil,
		},
		{
			name: "unknown function fallback",
			anonRule: maskingRule{
				schema:   "public",
				table:    "custom",
				column:   "data",
				function: "anon.custom_function(param1, param2)",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.custom_function(param1, param2)",
				},
			},
			wantErr: nil,
		},
		{
			name: "digest function with too many parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "users",
				column:   "ssn",
				function: "anon.digest(value, 'salt123', 'sha256', 'extra')",
			},
			wantRule: TransformerRules{},
			wantErr:  errMissingParameters,
		},
		{
			name: "dnoise function with insufficient parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "events",
				column:   "timestamp",
				function: "anon.dnoise(value)",
			},
			wantRule: TransformerRules{},
			wantErr:  errMissingParameters,
		},
		{
			name: "image_blur function with insufficient parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "photos",
				column:   "image_data",
				function: "anon.image_blur(data)",
			},
			wantRule: TransformerRules{},
			wantErr:  errMissingParameters,
		},
		{
			name: "partial function with too many parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "users",
				column:   "phone",
				function: "anon.partial(value, 3, 'X', 2, 'extra')",
			},
			wantRule: TransformerRules{},
			wantErr:  errMissingParameters,
		},
		{
			name: "random_in_int4range function with insufficient parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "products",
				column:   "price_range",
				function: "anon.random_in_int4range()",
			},
			wantRule: TransformerRules{},
			wantErr:  errMissingParameters,
		},
		{
			name: "random_string function with insufficient parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "tokens",
				column:   "token",
				function: "anon.random_string()",
			},
			wantRule: TransformerRules{},
			wantErr:  errMissingParameters,
		},
		{
			name: "random_int_between function with insufficient parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "ages",
				column:   "age",
				function: "anon.random_int_between(18)",
			},
			wantRule: TransformerRules{},
			wantErr:  errMissingParameters,
		},
		{
			name: "lorem_ipsum function with too many parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "articles",
				column:   "content",
				function: "anon.lorem_ipsum(words := 100 := extra)",
			},
			wantRule: TransformerRules{},
			wantErr:  errTooManyParameters,
		},
		{
			name: "random_in_enum function",
			anonRule: maskingRule{
				schema:   "public",
				table:    "products",
				column:   "category",
				function: "anon.random_in_enum(NULL::CATEGORY)",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.random_in_enum",
					"range":         "NULL::CATEGORY",
				},
			},
			wantErr: nil,
		},
		{
			name: "random_in_enum function missing parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "products",
				column:   "category",
				function: "anon.random_in_enum()",
			},
			wantRule: TransformerRules{},
			wantErr:  errMissingParameters,
		},
		{
			name: "random_date_between function",
			anonRule: maskingRule{
				schema:   "public",
				table:    "events",
				column:   "event_date",
				function: "anon.random_date_between('2020-01-01', '2023-12-31')",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.random_date_between",
					"min":           "2020-01-01",
					"max":           "2023-12-31",
				},
			},
			wantErr: nil,
		},
		{
			name: "random_bigint_between function",
			anonRule: maskingRule{
				schema:   "public",
				table:    "transactions",
				column:   "amount",
				function: "anon.random_bigint_between(1000, 999999)",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.random_bigint_between",
					"min":           "1000",
					"max":           "999999",
				},
			},
			wantErr: nil,
		},
		{
			name: "custom function with parameters",
			anonRule: maskingRule{
				schema:   "public",
				table:    "transactions",
				column:   "amount",
				function: "anon.custom_function(1000, 999999)",
			},
			wantRule: TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": "anon.custom_function(1000, 999999)",
				},
			},
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := &AnonRuleParser{}

			rules, err := a.anonMaskingToTransformerRules(tc.anonRule)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, rules, tc.wantRule)
		})
	}
}

func TestAnonRuleParser_trimParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		param string
		want  string
	}{
		{
			name:  "trim single quotes and spaces",
			param: " 'value' ",
			want:  "value",
		},
		{
			name:  "trim only spaces",
			param: "  value  ",
			want:  "value",
		},
		{
			name:  "trim only single quotes",
			param: "'value'",
			want:  "value",
		},
		{
			name:  "no trimming needed",
			param: "value",
			want:  "value",
		},
		{
			name:  "empty string",
			param: "",
			want:  "",
		},
		{
			name:  "only quotes and spaces",
			param: " '' ",
			want:  "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := trimParameter(tc.param)
			require.Equal(t, tc.want, result)
		})
	}
}
