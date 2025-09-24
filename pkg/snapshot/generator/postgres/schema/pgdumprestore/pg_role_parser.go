// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"bufio"
	"bytes"
	"strings"
)

type roleSQLParser struct{}

type role struct {
	name             string
	roleDependencies map[string]role
	isOwner          bool
}

type roleOption func(r *role)

var (
	// PostgreSQL provides a set of predefined roles that provide access to
	// certain, commonly needed, privileged capabilities and information
	// https://www.postgresql.org/docs/current/predefined-roles.html
	pgPredefinedRolesMap = map[string]struct{}{
		"pg_read_all_data":            {},
		"pg_write_all_data":           {},
		"pg_read_all_settings":        {},
		"pg_read_all_stats":           {},
		"pg_stat_scan_tables":         {},
		"pg_monitor":                  {},
		"pg_database_owner":           {},
		"pg_signal_backend":           {},
		"pg_read_server_files":        {},
		"pg_write_server_files":       {},
		"pg_execute_server_program":   {},
		"pg_checkpoint":               {},
		"pg_maintain":                 {},
		"pg_use_reserved_connections": {},
		"pg_create_subscription":      {},
		"postgres":                    {},
		"PUBLIC":                      {},
	}

	// Roles used by Google Cloud SQL that should not be included in the roles
	// dump since they are managed by Cloud SQL itself, and they require
	// superuser access which is not available.
	pgCloudSQLRoles = map[string]struct{}{
		"cloudsqladmin":                  {},
		"cloudsqlsuperuser":              {},
		"cloudsqlagent":                  {},
		"cloudsqlimportexport":           {},
		"cloudsqlreplica":                {},
		"cloudsqlobservability":          {},
		"cloudsqlconnpooladmin":          {},
		"cloudsqliamgroup":               {},
		"cloudsqliamgroupserviceaccount": {},
		"cloudsqliamgroupuser":           {},
		"cloudsqliamserviceaccount":      {},
		"cloudsqliamuser":                {},
		"cloudsqllogical":                {},
		"cloudsqlinactiveuser":           {},
	}
)

func newRole(name string, opts ...roleOption) role {
	r := role{
		name: name,
	}
	for _, opt := range opts {
		opt(&r)
	}
	return r
}

func withOwner() roleOption {
	return func(r *role) {
		r.isOwner = true
	}
}

func withRoleDeps(deps ...string) roleOption {
	return func(r *role) {
		r.roleDependencies = make(map[string]role)
		for _, dep := range deps {
			if dep != "" {
				r.roleDependencies[dep] = newRole(dep)
			}
		}
	}
}

func (p *roleSQLParser) extractRoleNamesFromDump(rolesDump []byte) map[string]role {
	scanner := bufio.NewScanner(bytes.NewReader(rolesDump))
	scanner.Split(bufio.ScanLines)
	var roles []role
	for scanner.Scan() {
		line := scanner.Text()
		if isRoleStatement(line) {
			roles = append(roles, p.extractRoleNamesFromLine(line)...)
		}
	}
	roleMap := make(map[string]role)
	for _, r := range roles {
		existingRole, found := roleMap[r.name]
		if !found {
			roleMap[r.name] = r
			continue
		}
		// Merge role dependencies and ownership if role already exists
		for depName, depRole := range r.roleDependencies {
			if existingRole.roleDependencies == nil {
				existingRole.roleDependencies = make(map[string]role)
			}
			existingRole.roleDependencies[depName] = depRole
		}
		if r.isOwner {
			existingRole.isOwner = true
		}
		roleMap[r.name] = existingRole
	}
	return roleMap
}

func (p *roleSQLParser) extractRoleNamesFromLine(line string) []role {
	var roles []role
	switch {
	case strings.HasPrefix(line, "DROP ROLE IF EXISTS "):
		roles = append(roles, newRole(getRoleNameAfterClause(line, "DROP ROLE IF EXISTS ")))

	case strings.HasPrefix(line, "DROP ROLE "):
		roles = append(roles, newRole(getRoleNameAfterClause(line, "DROP ROLE ")))

	case strings.HasPrefix(line, "CREATE ROLE "), strings.HasPrefix(line, "ALTER ROLE "), strings.HasPrefix(line, "COMMENT ON ROLE "):
		roles = append(roles, newRole(getRoleNameAfterClause(line, " ROLE ")))

	case strings.HasPrefix(line, "ALTER") && strings.Contains(line, "OWNER TO"):
		roles = append(roles, newRole(getRoleNameAfterClause(line, " OWNER TO "), withOwner()))

	case strings.HasPrefix(line, "GRANT "):
		grantorRole := ""
		if strings.Contains(line, " GRANTED BY ") {
			// GRANT ... GRANTED BY <rolename>;
			grantorRole = getRoleNameAfterClause(line, " GRANTED BY ")
		}

		roleName := getRoleNameAfterClause(line, "GRANT ")
		_, secondPart, _ := strings.Cut(line, roleName)
		if strings.HasPrefix(secondPart, " TO ") {
			// GRANT <rolename> TO <rolename2>;
			roleName2 := getRoleNameAfterClause(secondPart, " TO ")
			roles = append(roles,
				newRole(roleName, withRoleDeps(roleName2, grantorRole)),
				newRole(roleName2, withRoleDeps(roleName, grantorRole)),
			)
		} else if strings.Contains(line, " ON ") {
			// GRANT <privileges> ON <object> TO <rolename>;
			roles = append(roles, newRole(getRoleNameAfterClause(line, " TO "), withRoleDeps(grantorRole)))
		}

	case strings.HasPrefix(line, "REVOKE "):
		roles = append(roles, newRole(getRoleNameAfterClause(line, " FROM ")))

	case strings.HasPrefix(line, "SET ROLE "),
		strings.HasPrefix(line, "SET SESSION ROLE "),
		strings.HasPrefix(line, "SET LOCAL ROLE "):
		roles = append(roles, newRole(getRoleNameAfterClause(line, " ROLE ")))

	case strings.HasPrefix(line, "SET ") && strings.Contains(line, " SESSION AUTHORIZATION "):
		roleName := getRoleNameAfterClause(line, " SESSION AUTHORIZATION ")
		if roleName != "DEFAULT" {
			roles = append(roles, newRole(roleName))
		}

	case strings.HasPrefix(line, "ALTER DEFAULT PRIVILEGES FOR ROLE "):
		roleName := getRoleNameAfterClause(line, "ALTER DEFAULT PRIVILEGES FOR ROLE ")
		roles = append(roles, newRole(roleName))
		_, afterRole, _ := strings.Cut(line, roleName)
		_, afterRevoke, isRevokeStmt := strings.Cut(afterRole, " REVOKE ")
		if isRevokeStmt {
			// ALTER DEFAULT PRIVILEGES FOR ROLE <rolename> [IN SCHEMA <schemaname>] REVOKE <privileges> ON <objecttype> FROM <rolename2>;
			_, afterOn, _ := strings.Cut(afterRevoke, " ON ")
			roles = append(roles, role{name: getRoleNameAfterClause(afterOn, " FROM ")})
		} else {
			// ALTER DEFAULT PRIVILEGES FOR ROLE <rolename> [IN SCHEMA <schemaname>] GRANT <privileges> ON <objecttype> TO <rolename2>;
			_, afterGrant, _ := strings.Cut(afterRole, " GRANT ")
			_, afterOn, _ := strings.Cut(afterGrant, " ON ")
			roles = append(roles, role{name: getRoleNameAfterClause(afterOn, " TO ")})
		}
	}

	return roles
}

func getRoleNameAfterClause(line string, clause string) string {
	_, roleName, found := strings.Cut(line, clause)
	if !found {
		return ""
	}

	// Handle quoted role names
	if strings.HasPrefix(roleName, "\"") {
		if endIndex := strings.Index(roleName[1:], "\""); endIndex != -1 {
			return roleName[:endIndex+2] // +2 to include both quotes
		}
	}

	// Handle unquoted role names - stop at first space or semicolon
	if endIndex := strings.IndexAny(roleName, " ;"); endIndex != -1 {
		return roleName[:endIndex]
	}

	return roleName
}

func isPredefinedRole(roleName string) bool {
	_, found := pgPredefinedRolesMap[roleName]
	return found
}

// exclude roles managed by cloud services
func isExcludedRole(roleName string) bool {
	_, found := pgCloudSQLRoles[roleName]
	return found
}

func isRoleStatement(line string) bool {
	switch {
	case strings.HasPrefix(line, "CREATE ROLE "),
		strings.HasPrefix(line, "ALTER ROLE "),
		strings.HasPrefix(line, "DROP ROLE "),
		strings.HasPrefix(line, "DROP ROLE IF EXISTS "),
		strings.HasPrefix(line, "COMMENT ON ROLE "),
		strings.HasPrefix(line, "ALTER") && strings.Contains(line, "OWNER TO"), // Add this line
		strings.HasPrefix(line, "GRANT ") && strings.Contains(line, " TO "),
		strings.HasPrefix(line, "REVOKE ") && strings.Contains(line, " FROM "),
		strings.HasPrefix(line, "SET ROLE "),
		strings.HasPrefix(line, "SET SESSION ROLE "),
		strings.HasPrefix(line, "SET LOCAL ROLE "),
		strings.HasPrefix(line, "SET ") && strings.Contains(line, " SESSION AUTHORIZATION "),
		strings.HasPrefix(line, "ALTER DEFAULT PRIVILEGES FOR ROLE "):
		return true
	default:
		return false
	}
}

func hasExcludedRole(roles []role) bool {
	for _, r := range roles {
		if isExcludedRole(r.name) {
			return true
		}
	}
	return false
}

// remove role attributes that require superuser privileges to be set
// when the value is the same as the default.
func removeDefaultRoleAttributes(line string) string {
	line = strings.Replace(line, "NOSUPERUSER", "", 1)
	line = strings.Replace(line, "NOINHERIT", "", 1)
	line = strings.Replace(line, "NOCREATEROLE", "", 1)
	line = strings.Replace(line, "NOCREATEDB", "", 1)
	line = strings.Replace(line, "NOLOGIN", "", 1)
	line = strings.Replace(line, "NOREPLICATION", "", 1)
	line = strings.Replace(line, "NOBYPASSRLS", "", 1)
	return line
}
