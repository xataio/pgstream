// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoleSQLParser_extractRoleNamesFromLine(t *testing.T) {
	t.Parallel()

	parser := &roleSQLParser{}

	tests := []struct {
		name     string
		line     string
		expected []role
	}{
		{
			name: "DROP ROLE IF EXISTS",
			line: "DROP ROLE IF EXISTS testuser;",
			expected: []role{
				{name: "testuser", roleDependencies: nil, isOwner: false},
			},
		},
		{
			name: "DROP ROLE",
			line: "DROP ROLE testuser;",
			expected: []role{
				{name: "testuser", roleDependencies: nil, isOwner: false},
			},
		},
		{
			name: "CREATE ROLE",
			line: "CREATE ROLE testuser WITH LOGIN;",
			expected: []role{
				{name: "testuser", roleDependencies: nil, isOwner: false},
			},
		},
		{
			name: "ALTER ROLE",
			line: "ALTER ROLE testuser CREATEDB;",
			expected: []role{
				{name: "testuser", roleDependencies: nil, isOwner: false},
			},
		},
		{
			name: "COMMENT ON ROLE",
			line: "COMMENT ON ROLE testuser IS 'Test user';",
			expected: []role{
				{name: "testuser", roleDependencies: nil, isOwner: false},
			},
		},
		{
			name: "ALTER OWNER TO",
			line: "ALTER TABLE test OWNER TO testuser;",
			expected: []role{
				{name: "testuser", roleDependencies: nil, isOwner: true},
			},
		},
		{
			name: "GRANT role to role",
			line: "GRANT role1 TO role2;",
			expected: []role{
				{name: "role1", roleDependencies: map[string]role{"role2": {name: "role2"}}, isOwner: false},
				{name: "role2", roleDependencies: map[string]role{"role1": {name: "role1"}}, isOwner: false},
			},
		},
		{
			name: "GRANT privileges",
			line: "GRANT SELECT ON TABLE test TO testuser;",
			expected: []role{
				{name: "testuser", roleDependencies: map[string]role{}, isOwner: false},
			},
		},
		{
			name: "GRANT with GRANTED BY",
			line: "GRANT role1 TO role2 GRANTED BY grantor;",
			expected: []role{
				{name: "role1", roleDependencies: map[string]role{"role2": {name: "role2"}, "grantor": {name: "grantor"}}, isOwner: false},
				{name: "role2", roleDependencies: map[string]role{"role1": {name: "role1"}, "grantor": {name: "grantor"}}, isOwner: false},
			},
		},
		{
			name: "REVOKE",
			line: "REVOKE role1 FROM role2;",
			expected: []role{
				{name: "role2", roleDependencies: nil, isOwner: false},
			},
		},
		{
			name: "SET ROLE",
			line: "SET ROLE testuser;",
			expected: []role{
				{name: "testuser", roleDependencies: nil, isOwner: false},
			},
		},
		{
			name: "SET SESSION ROLE",
			line: "SET SESSION ROLE testuser;",
			expected: []role{
				{name: "testuser", roleDependencies: nil, isOwner: false},
			},
		},
		{
			name: "SET LOCAL ROLE",
			line: "SET LOCAL ROLE testuser;",
			expected: []role{
				{name: "testuser", roleDependencies: nil, isOwner: false},
			},
		},
		{
			name: "SET SESSION AUTHORIZATION",
			line: "SET SESSION AUTHORIZATION testuser;",
			expected: []role{
				{name: "testuser", roleDependencies: nil, isOwner: false},
			},
		},
		{
			name:     "SET SESSION AUTHORIZATION DEFAULT",
			line:     "SET SESSION AUTHORIZATION DEFAULT;",
			expected: nil,
		},
		{
			name: "ALTER DEFAULT PRIVILEGES GRANT",
			line: "ALTER DEFAULT PRIVILEGES FOR ROLE owner GRANT SELECT ON TABLES TO reader;",
			expected: []role{
				{name: "owner", roleDependencies: nil, isOwner: false},
				{name: "reader", roleDependencies: nil, isOwner: false},
			},
		},
		{
			name: "ALTER DEFAULT PRIVILEGES REVOKE",
			line: "ALTER DEFAULT PRIVILEGES FOR ROLE owner REVOKE SELECT ON TABLES FROM reader;",
			expected: []role{
				{name: "owner", roleDependencies: nil, isOwner: false},
				{name: "reader", roleDependencies: nil, isOwner: false},
			},
		},
		{
			name: "quoted role name",
			line: "CREATE ROLE \"test-user\" WITH LOGIN;",
			expected: []role{
				{name: "\"test-user\"", roleDependencies: nil, isOwner: false},
			},
		},
		{
			name:     "non-role statement",
			line:     "CREATE TABLE test (id int);",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := parser.extractRoleNamesFromLine(tt.line)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestRoleSQLParser_extractRoleNamesFromDump(t *testing.T) {
	t.Parallel()

	parser := &roleSQLParser{}

	tests := []struct {
		name     string
		dump     string
		expected map[string]role
	}{
		{
			name: "multiple role statements",
			dump: `CREATE ROLE user1 WITH LOGIN;
CREATE ROLE user2 WITH LOGIN;
CREATE ROLE user3 WITH PASSWORD 'password';
GRANT user1 TO user2 GRANTED BY user3;
ALTER TABLE test OWNER TO user1;`,
			expected: map[string]role{
				"user1": {name: "user1", roleDependencies: map[string]role{"user2": {name: "user2"}, "user3": {name: "user3"}}, isOwner: true},
				"user2": {name: "user2", roleDependencies: map[string]role{"user1": {name: "user1"}, "user3": {name: "user3"}}, isOwner: false},
				"user3": {name: "user3", roleDependencies: nil, isOwner: false},
			},
		},
		{
			name:     "empty dump",
			dump:     "",
			expected: map[string]role{},
		},
		{
			name: "no role statements",
			dump: `CREATE TABLE test (id int);
INSERT INTO test VALUES (1);`,
			expected: map[string]role{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := parser.extractRoleNamesFromDump([]byte(tt.dump))
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestGetRoleNameAfterClause(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		line     string
		clause   string
		expected string
	}{
		{
			name:     "simple role name",
			line:     "CREATE ROLE testuser WITH LOGIN;",
			clause:   " ROLE ",
			expected: "testuser",
		},
		{
			name:     "quoted role name",
			line:     "CREATE ROLE \"test-user\" WITH LOGIN;",
			clause:   " ROLE ",
			expected: "\"test-user\"",
		},
		{
			name:     "role name with semicolon",
			line:     "DROP ROLE testuser;",
			clause:   "DROP ROLE ",
			expected: "testuser",
		},
		{
			name:     "clause not found",
			line:     "CREATE TABLE test (id int);",
			clause:   " ROLE ",
			expected: "",
		},
		{
			name:     "malformed quoted name",
			line:     "CREATE ROLE \"testuser WITH LOGIN;",
			clause:   " ROLE ",
			expected: "\"testuser",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := getRoleNameAfterClause(tt.line, tt.clause)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestIsPredefinedRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		roleName string
		expected bool
	}{
		{"pg_read_all_data", "pg_read_all_data", true},
		{"postgres", "postgres", true},
		{"PUBLIC", "PUBLIC", true},
		{"custom_role", "custom_role", false},
		{"", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := isPredefinedRole(tt.roleName)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestIsExcludedRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		roleName string
		expected bool
	}{
		{"cloudsqladmin", "cloudsqladmin", true},
		{"cloudsqlsuperuser", "cloudsqlsuperuser", true},
		{"custom_role", "custom_role", false},
		{"", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := isExcludedRole(tt.roleName)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestIsRoleStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		line     string
		expected bool
	}{
		{"CREATE ROLE", "CREATE ROLE test;", true},
		{"ALTER ROLE", "ALTER ROLE test CREATEDB;", true},
		{"DROP ROLE", "DROP ROLE test;", true},
		{"DROP ROLE IF EXISTS", "DROP ROLE IF EXISTS test;", true},
		{"GRANT", "GRANT role1 TO role2;", true},
		{"REVOKE", "REVOKE role1 FROM role2;", true},
		{"SET ROLE", "SET ROLE test;", true},
		{"ALTER OWNER", "ALTER TABLE test OWNER TO user;", true},
		{"CREATE TABLE", "CREATE TABLE test (id int);", false},
		{"INSERT", "INSERT INTO test VALUES (1);", false},
		{"GRANT without TO", "GRANT SELECT;", false},
		{"REVOKE without FROM", "REVOKE SELECT;", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := isRoleStatement(tt.line)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestHasExcludedRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		roles    []role
		expected bool
	}{
		{
			name: "contains excluded role",
			roles: []role{
				{name: "user1"},
				{name: "cloudsqladmin"},
			},
			expected: true,
		},
		{
			name: "no excluded roles",
			roles: []role{
				{name: "user1"},
				{name: "user2"},
			},
			expected: false,
		},
		{
			name:     "empty roles",
			roles:    []role{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := hasExcludedRole(tt.roles)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestRemoveDefaultRoleAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		line     string
		expected string
	}{
		{
			name:     "remove NOSUPERUSER",
			line:     "CREATE ROLE test NOSUPERUSER;",
			expected: "CREATE ROLE test ;",
		},
		{
			name:     "remove multiple attributes",
			line:     "CREATE ROLE test NOSUPERUSER NOINHERIT NOCREATEROLE;",
			expected: "CREATE ROLE test   ;",
		},
		{
			name:     "no attributes to remove",
			line:     "CREATE ROLE test WITH LOGIN;",
			expected: "CREATE ROLE test WITH LOGIN;",
		},
		{
			name:     "remove all default attributes",
			line:     "CREATE ROLE test NOSUPERUSER NOINHERIT NOCREATEROLE NOCREATEDB NOLOGIN NOREPLICATION NOBYPASSRLS;",
			expected: "CREATE ROLE test       ;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := removeDefaultRoleAttributes(tt.line)
			require.Equal(t, tt.expected, result)
		})
	}
}
