// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibinstrumentation "github.com/xataio/pgstream/internal/postgres/instrumentation"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemaloginstrumentation "github.com/xataio/pgstream/pkg/schemalog/instrumentation"
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
)

// SnapshotGenerator generates postgres schema snapshots using pg_dump and
// pg_restore
type SnapshotGenerator struct {
	sourceURL              string
	targetURL              string
	pgDumpFn               pglib.PGDumpFn
	pgDumpAllFn            pglib.PGDumpAllFn
	pgRestoreFn            pglib.PGRestoreFn
	schemalogStore         schemalog.Store
	connBuilder            pglib.QuerierBuilder
	cleanTargetDB          bool
	createTargetDB         bool
	includeGlobalDBObjects bool
	role                   string
	rolesSnapshotMode      string
	logger                 loglib.Logger
	generator              generator.SnapshotGenerator
	dumpDebugFile          string // if set, the dump will be written to this file for debugging purposes
}

type Config struct {
	SourcePGURL    string
	TargetPGURL    string
	CleanTargetDB  bool
	CreateTargetDB bool
	// if set to true the snapshot will include all database objects, not tied
	// to any particular schema, such as extensions or triggers.
	IncludeGlobalDBObjects bool
	// Role name to be used to create the dump
	Role string
	// "enabled", "disabled", or "no_passwords"
	RolesSnapshotMode string
	// if set, the dump will be written to this file for debugging purposes
	DumpDebugFile string
}

type Option func(s *SnapshotGenerator)

type dump struct {
	full                  []byte
	filtered              []byte
	cleanupPart           []byte
	indicesAndConstraints []byte
	sequences             []string
	roles                 map[string]struct{}
}

const (
	publicSchema       = "public"
	wildcard           = "*"
	dropSchemaPgstream = "DROP SCHEMA IF EXISTS pgstream;"
)

// NewSnapshotGenerator will return a postgres schema snapshot generator that
// uses pg_dump and pg_restore to sync the schema of two postgres databases
func NewSnapshotGenerator(ctx context.Context, c *Config, opts ...Option) (*SnapshotGenerator, error) {
	sg := &SnapshotGenerator{
		sourceURL:              c.SourcePGURL,
		targetURL:              c.TargetPGURL,
		pgDumpFn:               pglib.RunPGDump,
		pgDumpAllFn:            pglib.RunPGDumpAll,
		pgRestoreFn:            pglib.RunPGRestore,
		connBuilder:            pglib.ConnBuilder,
		cleanTargetDB:          c.CleanTargetDB,
		createTargetDB:         c.CreateTargetDB,
		includeGlobalDBObjects: c.IncludeGlobalDBObjects,
		role:                   c.Role,
		rolesSnapshotMode:      c.RolesSnapshotMode,
		logger:                 loglib.NewNoopLogger(),
		dumpDebugFile:          c.DumpDebugFile,
	}

	if err := sg.initialiseSchemaLogStore(ctx); err != nil {
		return nil, err
	}

	for _, opt := range opts {
		opt(sg)
	}

	return sg, nil
}

func WithLogger(logger loglib.Logger) Option {
	return func(sg *SnapshotGenerator) {
		sg.logger = loglib.NewLogger(logger).WithFields(loglib.Fields{
			loglib.ModuleField: "postgres_schema_snapshot_generator",
		})
	}
}

func WithSnapshotGenerator(g generator.SnapshotGenerator) Option {
	return func(sg *SnapshotGenerator) {
		sg.generator = g
	}
}

func WithInstrumentation(i *otel.Instrumentation) Option {
	return func(sg *SnapshotGenerator) {
		var err error
		sg.connBuilder, err = pglibinstrumentation.NewQuerierBuilder(sg.connBuilder, i)
		if err != nil {
			// this should never happen
			panic(err)
		}

		sg.pgDumpFn = pglibinstrumentation.NewPGDumpFn(sg.pgDumpFn, i)
		sg.pgDumpAllFn = pglibinstrumentation.NewPGDumpAllFn(sg.pgDumpAllFn, i)
		sg.pgRestoreFn = pglibinstrumentation.NewPGRestoreFn(sg.pgRestoreFn, i)
		if sg.schemalogStore != nil {
			sg.schemalogStore = schemaloginstrumentation.NewStore(sg.schemalogStore, i)
		}
	}
}

func (s *SnapshotGenerator) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) error {
	s.logger.Info("creating schema snapshot", loglib.Fields{"schemaTables": ss.SchemaTables})

	// make sure any empty schemas are filtered out
	dumpSchemas := make(map[string][]string, len(ss.SchemaTables))
	for schema, tables := range ss.SchemaTables {
		if len(tables) > 0 {
			dumpSchemas[schema] = tables
		}
	}
	// nothing to dump
	if len(dumpSchemas) == 0 {
		return nil
	}

	dump, err := s.dumpSchema(ctx, dumpSchemas, ss.SchemaExcludedTables)
	if err != nil {
		return err
	}

	// the schema will include the sequences but will not produce the `SETVAL`
	// queries since that's considered data and it's a schema only dump. Produce
	// the data only dump for the sequences only and restore it along with the
	// schema.
	sequenceDump, err := s.dumpSequenceValues(ctx, dump.sequences)
	if err != nil {
		return err
	}

	// the schema dump will not include the roles, so we need to dump them
	// separately and restore them as well.
	rolesDump, err := s.dumpRoles(ctx, dump.roles)
	if err != nil {
		return err
	}

	// if there's no further snapshotting happening, we can apply the full dump,
	// no need to apply the constraints/indices separately.
	if s.generator == nil {
		// the cleanup part will drop all the objects before we execute the roles dump,
		// and this will allow us to drop the roles safely, in case `clean_target_db` is enabled.
		dumpsToRestore := dump.cleanupPart
		dumpsToRestore = append(dumpsToRestore, rolesDump...)
		dumpsToRestore = append(dumpsToRestore, dump.full...)
		dumpsToRestore = append(dumpsToRestore, sequenceDump...)
		return s.restoreDump(ctx, dumpSchemas, dumpsToRestore)
	}

	// otherwise, we need to apply the roles dump along with the filtered schema dump first,
	// then call the wrapped snapshot generator, and apply the indices and constraints last.
	// This will make the data snapshot faster, since there will be no
	// constraints to be updated/checked on each insert.
	if err := s.restoreDump(ctx, dumpSchemas, append(dump.cleanupPart, append(rolesDump, dump.filtered...)...)); err != nil {
		return err
	}

	if err := s.generator.CreateSnapshot(ctx, ss); err != nil {
		return err
	}

	s.logger.Info("restoring schema indices and constraints", loglib.Fields{"schemaTables": ss.SchemaTables})
	// apply the indices and constraints when the wrapped generator has finished
	return s.restoreDump(ctx, dumpSchemas, append(dump.indicesAndConstraints, sequenceDump...))
}

func (s *SnapshotGenerator) Close() error {
	if s.schemalogStore != nil {
		return s.schemalogStore.Close()
	}

	if s.generator != nil {
		return s.generator.Close()
	}

	return nil
}

func (s *SnapshotGenerator) dumpSchema(ctx context.Context, schemaTables map[string][]string, excludedTables map[string][]string) (*dump, error) {
	pgdumpOpts, err := s.pgdumpOptions(ctx, schemaTables, excludedTables)
	if err != nil {
		return nil, fmt.Errorf("preparing pg_dump options: %w", err)
	}

	s.logger.Debug("dumping schema", loglib.Fields{"pg_dump_options": pgdumpOpts.ToArgs(), "schema_tables": schemaTables})
	d, err := s.pgDumpFn(ctx, *pgdumpOpts)
	defer s.dumpToFile(s.dumpDebugFile, pgdumpOpts, d)
	if err != nil {
		s.logger.Error(err, "pg_dump for schema failed", loglib.Fields{"pgdumpOptions": pgdumpOpts.ToArgs()})
		return nil, fmt.Errorf("dumping schema: %w", err)
	}

	return s.parseDump(d), nil
}

func (s *SnapshotGenerator) dumpSequenceValues(ctx context.Context, sequences []string) ([]byte, error) {
	if len(sequences) == 0 {
		return nil, nil
	}

	opts := &pglib.PGDumpOptions{
		ConnectionString: s.sourceURL,
		Format:           "p",
		DataOnly:         true,
		Tables:           sequences,
	}

	s.logger.Debug("dumping sequence data", loglib.Fields{"pg_dump_options": opts.ToArgs(), "sequences": sequences})
	d, err := s.pgDumpFn(ctx, *opts)
	defer s.dumpToFile(s.sequenceDumpFile(), opts, d)
	if err != nil {
		s.logger.Error(err, "pg_dump for sequences failed", loglib.Fields{"pgdumpOptions": opts.ToArgs()})
		return nil, fmt.Errorf("dumping sequence values: %w", err)
	}
	return d, nil
}

func (s *SnapshotGenerator) dumpRoles(ctx context.Context, roles map[string]struct{}) ([]byte, error) {
	if s.rolesSnapshotMode == "disabled" {
		return nil, nil
	}
	opts := &pglib.PGDumpAllOptions{
		ConnectionString: s.sourceURL,
		RolesOnly:        true,
		Clean:            s.cleanTargetDB,
		Role:             s.role,
	}

	if s.rolesSnapshotMode == "no_passwords" {
		opts.NoPasswords = true
	}

	s.logger.Debug("dumping roles", loglib.Fields{"pg_dumpall_options": opts.ToArgs()})
	d, err := s.pgDumpAllFn(ctx, *opts)
	if err != nil {
		s.logger.Error(err, "pg_dumpall for roles failed", loglib.Fields{"pgdumpallOptions": opts.ToArgs()})
		return nil, fmt.Errorf("dumping roles: %w", err)
	}
	rolesToAdd := s.parseDump(d).roles
	for role := range rolesToAdd {
		roles[role] = struct{}{}
	}
	return filterRolesDump(d, roles), nil
}

func (s *SnapshotGenerator) restoreDump(ctx context.Context, schemaTables map[string][]string, dump []byte) error {
	// if we use table filtering in the pg_dump command, the schema creation
	// will not be dumped, so it needs to be created explicitly (except for
	// public schema)
	for schema, tables := range schemaTables {
		if len(tables) > 0 && schema != publicSchema && schema != wildcard {
			if err := s.createSchemaIfNotExists(ctx, schema); err != nil {
				return err
			}
		}
	}

	_, err := s.pgRestoreFn(ctx, s.pgrestoreOptions(), dump)
	pgrestoreErr := &pglib.PGRestoreErrors{}
	if err != nil {
		switch {
		case errors.As(err, &pgrestoreErr):
			if pgrestoreErr.HasCriticalErrors() {
				return err
			}
			ignoredErrors := pgrestoreErr.GetIgnoredErrors()
			s.logger.Warn(nil, fmt.Sprintf("restore: %d errors ignored", len(ignoredErrors)), loglib.Fields{"errors_ignored": ignoredErrors})
		default:
			return err
		}
	}

	// if we perform a schema snapshot using pg_dump/pg_restore, we need to make
	// sure the schema_log table is updated accordingly with the schema view so
	// that replication can work as expected if configured.
	if s.schemalogStore != nil {
		for schema := range schemaTables {
			if _, err := s.schemalogStore.Insert(ctx, schema); err != nil {
				return fmt.Errorf("inserting schemalog entry after schema snapshot: %w", err)
			}
		}
	}

	return nil
}

func (s *SnapshotGenerator) createSchemaIfNotExists(ctx context.Context, schemaName string) error {
	targetConn, err := s.connBuilder(ctx, s.targetURL)
	if err != nil {
		return err
	}
	defer targetConn.Close(context.Background())

	_, err = targetConn.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaName))
	return err
}

func (s *SnapshotGenerator) pgrestoreOptions() pglib.PGRestoreOptions {
	return pglib.PGRestoreOptions{
		ConnectionString: s.targetURL,
		Clean:            s.cleanTargetDB,
		Format:           "p",
		Create:           s.createTargetDB,
	}
}

func (s *SnapshotGenerator) pgdumpOptions(ctx context.Context, schemaTables map[string][]string, excludedTables map[string][]string) (*pglib.PGDumpOptions, error) {
	schemas := make([]string, 0, len(schemaTables))
	for schema := range schemaTables {
		schemas = append(schemas, schema)
	}
	opts := &pglib.PGDumpOptions{
		ConnectionString: s.sourceURL,
		Format:           "p",
		SchemaOnly:       true,
		Schemas:          schemas,
		Clean:            s.cleanTargetDB,
		Create:           s.createTargetDB,
	}

	if s.role != "" {
		opts.Role = s.role
		opts.NoOwner = true
	}

	switch {
	case hasWildcardSchema(schemaTables):
		// no need to filter schemas, since we are including all of them
		opts.Schemas = nil
	case s.includeGlobalDBObjects:
		// instead of using the schema filter, we use the exclude schemas filter
		// to make sure extensions and other database global objects are
		// created. pg_dump will not include them when using the schema filter,
		// since they do not belong to the schema.
		var err error
		opts.ExcludeSchemas, err = s.pgdumpExcludedSchemas(ctx, schemas)
		if err != nil {
			return nil, err
		}
		opts.Schemas = nil
	}

	// we use the excluded tables flag to make sure we still dump non table
	// objects for the schema in question. If we use the tables filter, only
	// those tables are dumped, and any related non table objects will not be
	// dumped, causing the restore to fail due to missing related objects.
	for schema, tables := range schemaTables {
		if hasWildcardTable(tables) {
			// if there's the wildcard table, we don't need to add excluded
			// tables, since they are all included.
			continue
		}
		var err error
		opts.ExcludeTables, err = s.pgdumpExcludedTables(ctx, schema, tables)
		if err != nil {
			return nil, err
		}
	}

	for schema, tables := range excludedTables {
		for _, table := range tables {
			if !slices.Contains(opts.ExcludeTables, pglib.QuoteQualifiedIdentifier(schema, table)) {
				opts.ExcludeTables = append(opts.ExcludeTables, pglib.QuoteQualifiedIdentifier(schema, table))
			}
		}
	}

	return opts, nil
}

const (
	selectTablesQuery       = "SELECT schemaname,tablename FROM pg_tables WHERE tablename NOT IN (%s)"
	selectSchemaTablesQuery = "SELECT schemaname,tablename FROM pg_tables WHERE schemaname = '%s' AND tablename NOT IN (%s)"
)

func (s *SnapshotGenerator) pgdumpExcludedTables(ctx context.Context, schemaName string, includeTables []string) ([]string, error) {
	conn, err := s.connBuilder(ctx, s.sourceURL)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	paramRefs := make([]string, 0, len(includeTables))
	tableParams := make([]any, 0, len(includeTables))
	for i, table := range includeTables {
		tableParams = append(tableParams, table)
		paramRefs = append(paramRefs, fmt.Sprintf("$%d", i+1))
	}

	var query string
	switch schemaName {
	case wildcard:
		query = fmt.Sprintf(selectTablesQuery, strings.Join(paramRefs, ","))
	default:
		// if the schema is not wildcard, we need to filter by schema name
		query = fmt.Sprintf(selectSchemaTablesQuery, schemaName, strings.Join(paramRefs, ","))
	}

	// get all tables in the schema that are not in the include list
	rows, err := conn.Query(ctx, query, tableParams...)
	if err != nil {
		return nil, fmt.Errorf("retrieving tables from schema: %w", err)
	}
	defer rows.Close()

	excludeTables := []string{}
	for rows.Next() {
		schemaName, tableName := "", ""
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return nil, fmt.Errorf("scanning table name: %w", err)
		}
		excludeTables = append(excludeTables, pglib.QuoteQualifiedIdentifier(schemaName, tableName))
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return excludeTables, nil
}

const selectSchemasQuery = "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN (%s)"

func (s *SnapshotGenerator) pgdumpExcludedSchemas(ctx context.Context, includeSchemas []string) ([]string, error) {
	conn, err := s.connBuilder(ctx, s.sourceURL)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	paramRefs := make([]string, 0, len(includeSchemas))
	schemaParams := make([]any, 0, len(includeSchemas))
	for i, schema := range includeSchemas {
		schemaParams = append(schemaParams, schema)
		paramRefs = append(paramRefs, fmt.Sprintf("$%d", i+1))
	}

	// get all schemas in the database that are not in the snapshot request
	query := fmt.Sprintf(selectSchemasQuery, strings.Join(paramRefs, ","))
	rows, err := conn.Query(ctx, query, schemaParams...)
	if err != nil {
		return nil, fmt.Errorf("retrieving schemas: %w", err)
	}
	defer rows.Close()

	excludeSchemas := []string{}
	for rows.Next() {
		schema := ""
		if err := rows.Scan(&schema); err != nil {
			return nil, fmt.Errorf("scanning schema name: %w", err)
		}
		excludeSchemas = append(excludeSchemas, pglib.QuoteIdentifier(schema))
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return excludeSchemas, nil
}

func (s *SnapshotGenerator) initialiseSchemaLogStore(ctx context.Context) error {
	exists, err := s.schemalogExists(ctx)
	if err != nil {
		return err
	}

	if !exists {
		return nil
	}

	s.schemalogStore, err = schemalogpg.NewStore(ctx, schemalogpg.Config{URL: s.sourceURL})
	return err
}

const existsTableQuery = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)"

func (s *SnapshotGenerator) schemalogExists(ctx context.Context) (bool, error) {
	conn, err := s.connBuilder(ctx, s.sourceURL)
	if err != nil {
		return false, err
	}
	defer conn.Close(context.Background())

	// check if the pgstream.schema_log table exists, if not, we can skip the initialisation
	// of the schemalog store
	var exists bool
	err = conn.QueryRow(ctx, existsTableQuery, schemalog.SchemaName, schemalog.TableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("checking schemalog table existence: %w", err)
	}

	return exists, nil
}

func (s *SnapshotGenerator) parseDump(d []byte) *dump {
	scanner := bufio.NewScanner(bytes.NewReader(d))
	scanner.Split(bufio.ScanLines)
	indicesAndConstraints := strings.Builder{}
	filteredDump := strings.Builder{}
	sequenceNames := []string{}
	roleNames := make(map[string]struct{})
	alterTable := ""
	cleanupPart := strings.Builder{}
	pgstreamDropSchemaFound := false
	pgstreamDropSchemaBlankLineFound := false
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case alterTable != "":
			// check if the previous alter table line is split in two lines and matches a constraint
			if strings.Contains(line, "ADD CONSTRAINT") {
				indicesAndConstraints.WriteString(alterTable)
				indicesAndConstraints.WriteString("\n")
				indicesAndConstraints.WriteString(line)
				indicesAndConstraints.WriteString("\n\n")
				alterTable = ""
			} else {
				filteredDump.WriteString(alterTable)
				filteredDump.WriteString("\n")
				filteredDump.WriteString(line)
				filteredDump.WriteString("\n")
				alterTable = ""
			}
		case strings.Contains(line, `\connect`):
			indicesAndConstraints.WriteString(line)
			indicesAndConstraints.WriteString("\n\n")
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		case strings.HasPrefix(line, "CREATE INDEX"),
			strings.HasPrefix(line, "CREATE UNIQUE INDEX"),
			strings.HasPrefix(line, "CREATE CONSTRAINT"),
			strings.HasPrefix(line, "CREATE TRIGGER"),
			strings.HasPrefix(line, "COMMENT ON CONSTRAINT"),
			strings.HasPrefix(line, "COMMENT ON INDEX"),
			strings.HasPrefix(line, "COMMENT ON TRIGGER"):
			indicesAndConstraints.WriteString(line)
			indicesAndConstraints.WriteString("\n\n")
		case strings.HasPrefix(line, "ALTER TABLE") && strings.Contains(line, "ADD CONSTRAINT"):
			indicesAndConstraints.WriteString(line)
		case strings.HasPrefix(line, "ALTER TABLE") && !strings.HasSuffix(line, ";"):
			// keep it in case the alter table is provided in two lines (pg_dump format)
			alterTable = line
		case strings.HasPrefix(line, "ALTER") && strings.Contains(line, "OWNER TO"):
			roleNames[getRoleNameAfterClause(line, " OWNER TO ")] = struct{}{}
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		case strings.HasPrefix(line, "GRANT "):
			roleName := getRoleNameAfterClause(line, "GRANT ")
			_, secondPart, _ := strings.Cut(line, roleName)
			if strings.HasPrefix(secondPart, " TO ") {
				// GRANT <rolename> TO <rolename2>;
				roleName2 := getRoleNameAfterClause(secondPart, " TO ")
				roleNames[roleName] = struct{}{}
				roleNames[roleName2] = struct{}{}
			} else if strings.Contains(line, " ON ") {
				// GRANT <privileges> ON <object> TO <rolename>;
				roleNames[getRoleNameAfterClause(line, " TO ")] = struct{}{}
			}
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		case strings.HasPrefix(line, "REVOKE "):
			roleNames[getRoleNameAfterClause(line, " FROM ")] = struct{}{}
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		case strings.HasPrefix(line, "SET ROLE "),
			strings.HasPrefix(line, "SET SESSION ROLE "),
			strings.HasPrefix(line, "SET LOCAL ROLE "):
			roleNames[getRoleNameAfterClause(line, " ROLE ")] = struct{}{}
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		case strings.HasPrefix(line, "SET ") && strings.Contains(line, " SESSION AUTHORIZATION "):
			roleName := getRoleNameAfterClause(line, " SESSION AUTHORIZATION ")
			if roleName != "DEFAULT" {
				roleNames[roleName] = struct{}{}
			}
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		case strings.HasPrefix(line, "ALTER DEFAULT PRIVILEGES FOR ROLE "):
			roleName := getRoleNameAfterClause(line, "ALTER DEFAULT PRIVILEGES FOR ROLE ")
			roleNames[roleName] = struct{}{}
			_, afterRole, _ := strings.Cut(line, roleName)
			_, afterRevoke, isRevokeStmt := strings.Cut(afterRole, " REVOKE ")
			if isRevokeStmt {
				// ALTER DEFAULT PRIVILEGES FOR ROLE <rolename> [IN SCHEMA <schemaname>] REVOKE <privileges> ON <objecttype> FROM <rolename2>;
				_, afterOn, _ := strings.Cut(afterRevoke, " ON ")
				roleNames[getRoleNameAfterClause(afterOn, " FROM ")] = struct{}{}
			} else {
				// ALTER DEFAULT PRIVILEGES FOR ROLE <rolename> [IN SCHEMA <schemaname>] GRANT <privileges> ON <objecttype> TO <rolename2>;
				_, afterGrant, _ := strings.Cut(afterRole, " GRANT ")
				_, afterOn, _ := strings.Cut(afterGrant, " ON ")
				roleNames[getRoleNameAfterClause(afterOn, " TO ")] = struct{}{}
			}
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		case strings.HasPrefix(line, "CREATE SEQUENCE"):
			qualifiedName, err := pglib.NewQualifiedName(strings.TrimPrefix(line, "CREATE SEQUENCE "))
			if err == nil {
				sequenceNames = append(sequenceNames, qualifiedName.String())
			}
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		default:
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		}

		if s.cleanTargetDB {
			/*
			 * With --clean option, pg_dump output looks like:
			 * -- some comments
			 *
			 * SET ...
			 * SELECT ...
			 * SET ...
			 *
			 * DROP ...
			 * ALTER TABLE .. DROP ..
			 * DROP SCHEMA IF EXISTS pgstream;
			 * DROP ...
			 *
			 * -- some comments
			 * CREATE ...
			 * -- the rest of the dump..
			 *
			 * Here we need to catch the part before CREATE statements begin.
			 * First we look for "DROP SCHEMA IF EXISTS pgstream;" because it will always be there.
			 * After that we look for a blank line (either empty or containing comments) that will mark the end of the cleanup part.
			 */
			if line == dropSchemaPgstream {
				pgstreamDropSchemaFound = true
			}
			if pgstreamDropSchemaFound && isBlankLine(line) {
				pgstreamDropSchemaBlankLineFound = true
			}
			if !pgstreamDropSchemaFound || !pgstreamDropSchemaBlankLineFound {
				cleanupPart.WriteString(line)
				cleanupPart.WriteString("\n")
			}
		}
	}

	delete(roleNames, "postgres") // remove the postgres role from the list, since it is not needed and can cause issues
	return &dump{
		full:                  d,
		filtered:              []byte(filteredDump.String()),
		indicesAndConstraints: []byte(indicesAndConstraints.String()),
		sequences:             sequenceNames,
		roles:                 roleNames,
		cleanupPart:           []byte(cleanupPart.String()),
	}
}

func filterRolesDump(rolesDump []byte, roles map[string]struct{}) []byte {
	scanner := bufio.NewScanner(bytes.NewReader(rolesDump))
	scanner.Split(bufio.ScanLines)
	var filteredDump strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		roleName := ""
		switch {
		case strings.HasPrefix(line, "DROP ROLE IF EXISTS "):
			roleName = getRoleNameAfterClause(line, "DROP ROLE IF EXISTS ")
		case strings.HasPrefix(line, "DROP ROLE "):
			roleName = getRoleNameAfterClause(line, "DROP ROLE ")
		case strings.HasPrefix(line, "CREATE ROLE "), strings.HasPrefix(line, "ALTER ROLE "), strings.HasPrefix(line, "COMMENT ON ROLE "):
			roleName = getRoleNameAfterClause(line, " ROLE ")
		}
		if roleName != "" {
			if _, ok := roles[roleName]; !ok {
				// skip the role if it is not in the roles map
				continue
			}
		}
		filteredDump.WriteString(line)
		filteredDump.WriteString("\n")
	}
	return []byte(filteredDump.String())
}

func getRoleNameAfterClause(line string, clause string) string {
	_, roleName, found := strings.Cut(line, clause)
	if !found {
		return ""
	}

	if strings.HasPrefix(roleName, "\"") {
		endIndexRelative := strings.Index(roleName[1:], "\"")
		if endIndexRelative != -1 {
			endIndexAbsolute := endIndexRelative + 1 // +1 to account for the leading quote
			roleName = roleName[:endIndexAbsolute+1]
		}
	} else {
		endIndex := strings.Index(roleName, " ")
		if endIndex != -1 {
			roleName = roleName[:endIndex]
		}
	}
	return strings.TrimSuffix(roleName, ";")
}

func (s *SnapshotGenerator) dumpToFile(file string, opts *pglib.PGDumpOptions, d []byte) {
	if s.dumpDebugFile != "" {
		b := bytes.NewBufferString(fmt.Sprintf("pg_dump options: %v\n\n%s", opts.ToArgs(), string(d)))
		if err := os.WriteFile(file, b.Bytes(), 0o644); err != nil { //nolint:gosec
			s.logger.Error(err, fmt.Sprintf("writing dump to debug file %s", file))
		}
	}
}

func (s *SnapshotGenerator) sequenceDumpFile() string {
	if s.dumpDebugFile == "" {
		return ""
	}

	fileExtension := filepath.Ext(s.dumpDebugFile)
	if fileExtension == "" {
		// if there's no extension, we assume it's a plain text file
		return s.dumpDebugFile + "-sequences"
	}

	// if there's an extension, we append "-sequences" before the extension
	baseName := strings.TrimSuffix(s.dumpDebugFile, fileExtension)
	return baseName + "-sequences" + fileExtension
}

func hasWildcardTable(tables []string) bool {
	return slices.Contains(tables, wildcard)
}

func hasWildcardSchema(schemaTables map[string][]string) bool {
	return schemaTables[wildcard] != nil
}

func isBlankLine(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "--") {
		return true
	}
	return false
}
