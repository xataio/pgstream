// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestMapError(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantErr error
	}{
		{
			name:    "generic error",
			err:     errors.New("some error"),
			wantErr: errors.New("some error"),
		},
		{
			name: "XX000 with cache lookup failed message",
			err: &pgconn.PgError{
				Code:    "XX000",
				Message: "cache lookup failed for attribute 1 of relation 16437",
			},
			wantErr: &ErrCacheLookupFailed{},
		},
		{
			name: "XX000 other internal message",
			err: &pgconn.PgError{
				Code:    "XX000",
				Message: "some other internal error",
			},
			wantErr: &pgconn.PgError{
				Code:    "XX000",
				Message: "some other internal error",
			},
		},
		{
			name: "42P01 undefined_table",
			err: &pgconn.PgError{
				Code:    "42P01",
				Message: "relation \"users\" does not exist",
			},
			wantErr: &ErrRelationDoesNotExist{},
		},
		{
			name: "42703 undefined_column",
			err: &pgconn.PgError{
				Code:    "42703",
				Message: "column \"age\" does not exist",
			},
			wantErr: &ErrRelationDoesNotExist{},
		},
		{
			name: "42883 undefined_function",
			err: &pgconn.PgError{
				Code:    "42883",
				Message: "function foo() does not exist",
			},
			wantErr: &ErrRelationDoesNotExist{},
		},
		{
			name: "42P02 undefined_parameter",
			err: &pgconn.PgError{
				Code:    "42P02",
				Message: "parameter $1 does not exist",
			},
			wantErr: &ErrRelationDoesNotExist{},
		},
		{
			name: "42704 undefined_object",
			err: &pgconn.PgError{
				Code:    "42704",
				Message: "type \"custom_type\" does not exist",
			},
			wantErr: &ErrRelationDoesNotExist{},
		},
		{
			name: "42601 syntax_error",
			err: &pgconn.PgError{
				Code:    "42601",
				Message: "syntax error at or near \"SELCT\"",
			},
			wantErr: &ErrSyntaxError{},
		},
		{
			name: "42000 syntax_error_or_access_rule_violation",
			err: &pgconn.PgError{
				Code:    "42000",
				Message: "syntax error or access rule violation",
			},
			wantErr: &ErrSyntaxError{},
		},
		{
			name: "42809 wrong_object_type",
			err: &pgconn.PgError{
				Code:    "42809",
				Message: "\"users\" is not a sequence",
			},
			wantErr: &ErrRuleViolation{},
		},
		{
			name: "428C9 generated_always",
			err: &pgconn.PgError{
				Code:    "428C9",
				Message: "cannot insert into column \"id\"",
			},
			wantErr: &ErrRuleViolation{},
		},
		{
			name: "42939 reserved_name",
			err: &pgconn.PgError{
				Code:    "42939",
				Message: "reserved name cannot be used",
			},
			wantErr: &ErrRuleViolation{},
		},
		{
			name: "42501 insufficient_privilege",
			err: &pgconn.PgError{
				Code:    "42501",
				Message: "permission denied for table users",
			},
			wantErr: &ErrPermissionDenied{},
		},
		{
			name: "55000 object_not_in_prerequisite_state",
			err: &pgconn.PgError{
				Code:    "55000",
				Message: "cannot drop table because other objects depend on it",
			},
			wantErr: &ErrPreconditionFailed{},
		},
		{
			name: "42P07 duplicate_table",
			err: &pgconn.PgError{
				Code:    "42P07",
				Message: "relation \"users\" already exists",
			},
			wantErr: &ErrRelationAlreadyExists{},
		},
		{
			name: "42701 duplicate_column",
			err: &pgconn.PgError{
				Code:    "42701",
				Message: "column \"name\" specified more than once",
			},
			wantErr: &ErrRelationAlreadyExists{},
		},
		{
			name: "42P03 duplicate_cursor",
			err: &pgconn.PgError{
				Code:    "42P03",
				Message: "cursor \"my_cursor\" already exists",
			},
			wantErr: &ErrRelationAlreadyExists{},
		},
		{
			name: "42P04 duplicate_database",
			err: &pgconn.PgError{
				Code:    "42P04",
				Message: "database \"mydb\" already exists",
			},
			wantErr: &ErrRelationAlreadyExists{},
		},
		{
			name: "42723 duplicate_function",
			err: &pgconn.PgError{
				Code:    "42723",
				Message: "function \"myfunc\" already exists",
			},
			wantErr: &ErrRelationAlreadyExists{},
		},
		{
			name: "42P05 duplicate_prepared_statement",
			err: &pgconn.PgError{
				Code:    "42P05",
				Message: "prepared statement \"stmt\" already exists",
			},
			wantErr: &ErrRelationAlreadyExists{},
		},
		{
			name: "42P06 duplicate_schema",
			err: &pgconn.PgError{
				Code:    "42P06",
				Message: "schema \"myschema\" already exists",
			},
			wantErr: &ErrRelationAlreadyExists{},
		},
		{
			name: "42712 duplicate_alias",
			err: &pgconn.PgError{
				Code:    "42712",
				Message: "table name \"t\" specified more than once",
			},
			wantErr: &ErrRelationAlreadyExists{},
		},
		{
			name: "42710 duplicate_object",
			err: &pgconn.PgError{
				Code:    "42710",
				Message: "object \"myobj\" already exists",
			},
			wantErr: &ErrRelationAlreadyExists{},
		},
		{
			name: "22001 string_data_right_truncation",
			err: &pgconn.PgError{
				Code:    "22001",
				Message: "value too long for type character varying(10)",
			},
			wantErr: &ErrDataException{},
		},
		{
			name: "22P02 invalid_text_representation",
			err: &pgconn.PgError{
				Code:    "22P02",
				Message: "invalid input syntax for type integer",
			},
			wantErr: &ErrDataException{},
		},
		{
			name: "23505 unique_violation",
			err: &pgconn.PgError{
				Code:    "23505",
				Message: "duplicate key value violates unique constraint",
			},
			wantErr: &ErrConstraintViolation{},
		},
		{
			name: "23503 foreign_key_violation",
			err: &pgconn.PgError{
				Code:    "23503",
				Message: "insert or update on table violates foreign key constraint",
			},
			wantErr: &ErrConstraintViolation{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mappedErr := MapError(tt.err)
			require.ErrorAs(t, mappedErr, &tt.wantErr)
		})
	}
}
