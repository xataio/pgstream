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
	logger                 loglib.Logger
	generator              generator.SnapshotGenerator
	dumpDebugFile          string
	excludedSecurityLabels []string
	roleSQLParser          *roleSQLParser
	optionGenerator        *optionGenerator
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
	// Do not output commands to set ownership of objects to match the original
	// database.
	NoOwner bool
	// Prevent dumping of access privileges (grant/revoke commands)
	NoPrivileges bool
	// if set, the dump will be written to this file for debugging purposes
	DumpDebugFile string
	// if set, security label providers that will be excluded from the dump
	ExcludedSecurityLabels []string
}

type Option func(s *SnapshotGenerator)

type dump struct {
	full                  []byte
	filtered              []byte
	cleanupPart           []byte
	indicesAndConstraints []byte
	sequences             []string
	roles                 map[string]role
}

const (
	publicSchema = "public"
	wildcard     = "*"
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
		logger:                 loglib.NewNoopLogger(),
		dumpDebugFile:          c.DumpDebugFile,
		excludedSecurityLabels: c.ExcludedSecurityLabels,
		roleSQLParser:          &roleSQLParser{},
		optionGenerator:        newOptionGenerator(pglib.ConnBuilder, c),
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

func (s *SnapshotGenerator) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) (err error) {
	s.logger.Info("creating schema snapshot", loglib.Fields{"schemaTables": ss.SchemaTables})
	defer func() {
		if err == nil {
			// if we perform a schema snapshot using pg_dump/pg_restore, we need to make
			// sure the schema_log table is updated accordingly with the schema view so that
			// replication can work as expected if configured.
			err = s.syncSchemaLog(ctx, ss.SchemaTables, ss.SchemaExcludedTables)
		}
	}()

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

	// the cleanup part will drop all the objects before we execute the roles dump,
	// and this will allow us to drop the roles safely, in case `clean_target_db` is enabled.
	preDataDump := dump.cleanupPart
	preDataDump = append(preDataDump, rolesDump...)
	preDataDump = append(preDataDump, dump.filtered...)

	postDataDump := sequenceDump
	postDataDump = append(postDataDump, dump.indicesAndConstraints...)

	// if there's no further snapshotting happening, we can apply the full dump,
	// no need to wait to apply the constraints/indices.
	if s.generator == nil {
		dumpsToRestore := preDataDump
		dumpsToRestore = append(dumpsToRestore, postDataDump...)
		return s.restoreDump(ctx, dumpSchemas, dumpsToRestore)
	}

	// otherwise, we need to apply the roles dump along with the filtered schema dump first,
	// then call the wrapped snapshot generator, and apply the indices and constraints last.
	// This will make the data snapshot faster, since there will be no
	// constraints to be updated/checked on each insert.
	if err := s.restoreDump(ctx, dumpSchemas, preDataDump); err != nil {
		return err
	}

	if err := s.generator.CreateSnapshot(ctx, ss); err != nil {
		return err
	}

	s.logger.Info("restoring schema indices and constraints", loglib.Fields{"schemaTables": ss.SchemaTables})
	// apply the indices and constraints when the wrapped generator has finished
	return s.restoreDump(ctx, dumpSchemas, postDataDump)
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
	pgdumpOpts, err := s.optionGenerator.pgdumpOptions(ctx, schemaTables, excludedTables)
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

	parsedDump := s.parseDump(d)

	if pgdumpOpts.Clean {
		// In case clean is enabled, we need the cleanup part of the dump separately, which will be restored before the roles dump.
		// This will allow us to drop the roles safely, without getting dependency erros.
		pgdumpOpts.Clean = false
		s.logger.Debug("dumping schema again without clean", loglib.Fields{"pg_dump_options": pgdumpOpts.ToArgs(), "schema_tables": schemaTables})
		dumpWithoutClean, err := s.pgDumpFn(ctx, *pgdumpOpts)
		if err != nil {
			s.logger.Error(err, "pg_dump for schema failed", loglib.Fields{"pgdumpOptions": pgdumpOpts.ToArgs()})
			return nil, fmt.Errorf("dumping schema: %w", err)
		}
		parsedDump.cleanupPart = getDumpsDiff(d, dumpWithoutClean)
	}

	s.dumpToFile(s.getDumpFileName("-filtered"), pgdumpOpts, parsedDump.filtered)

	return parsedDump, nil
}

func (s *SnapshotGenerator) dumpSequenceValues(ctx context.Context, sequences []string) ([]byte, error) {
	opts := s.optionGenerator.pgdumpSequenceDataOptions(sequences)
	if opts == nil {
		return nil, nil
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

func (s *SnapshotGenerator) dumpRoles(ctx context.Context, rolesInSchemaDump map[string]role) ([]byte, error) {
	opts := s.optionGenerator.pgdumpRolesOptions()
	if opts == nil {
		return nil, nil
	}

	// 1. dump all roles in the database
	s.logger.Debug("dumping roles", loglib.Fields{"pg_dumpall_options": opts.ToArgs()})
	d, err := s.pgDumpAllFn(ctx, *opts)
	if err != nil {
		s.logger.Error(err, "pg_dumpall for roles failed", loglib.Fields{"pgdumpallOptions": opts.ToArgs()})
		return nil, fmt.Errorf("dumping roles: %w", err)
	}

	// 2. extract the role names from the dump
	rolesInRoleDump := s.roleSQLParser.extractRoleNamesFromDump(d)

	s.logger.Debug("dumped roles", loglib.Fields{"roles in schema dump": rolesInSchemaDump, "roles in role dump": rolesInRoleDump})

	// 3. add any dependencies found in the role dump for the schema dump roles
	for _, role := range rolesInRoleDump {
		if _, found := rolesInSchemaDump[role.name]; !found {
			// if the role is not in the schema dump, we don't need to include its dependencies
			continue
		}
		for _, dep := range role.roleDependencies {
			rolesInSchemaDump[dep.name] = dep
		}
	}

	// 4. filter the dump statements to include only the roles found in the
	// schema dump and their dependencies
	filteredRolesDump := s.filterRolesDump(d, rolesInSchemaDump)
	s.dumpToFile(s.rolesDumpFile(), opts, filteredRolesDump)

	return filteredRolesDump, nil
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

	_, err := s.pgRestoreFn(ctx, s.optionGenerator.pgrestoreOptions(), dump)
	pgrestoreErr := &pglib.PGRestoreErrors{}
	if err != nil {
		switch {
		case errors.As(err, &pgrestoreErr):
			if pgrestoreErr.HasCriticalErrors() {
				return err
			}
			ignoredErrors := pgrestoreErr.GetIgnoredErrors()
			s.logger.Warn(err, fmt.Sprintf("restore: %d errors ignored", len(ignoredErrors)), loglib.Fields{"errors_ignored": ignoredErrors})
		default:
			return err
		}
	}

	return nil
}

func (s *SnapshotGenerator) syncSchemaLog(ctx context.Context, schemaTables, excludeSchemaTables map[string][]string) error {
	if s.schemalogStore == nil {
		return nil
	}

	s.logger.Info("syncing schema log", loglib.Fields{"schemaTables": schemaTables, "excludeSchemaTables": excludeSchemaTables})

	conn, err := s.connBuilder(ctx, s.sourceURL)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	schemas := make([]string, 0, len(schemaTables))
	switch {
	case hasWildcardSchema(schemaTables):
		schemas, err = pglib.DiscoverAllSchemas(ctx, conn)
		if err != nil {
			return fmt.Errorf("discovering schemas: %w", err)
		}
	default:
		for schema := range schemaTables {
			schemas = append(schemas, schema)
		}
	}

	for _, schema := range schemas {
		if _, found := excludeSchemaTables[schema]; found {
			continue
		}
		if _, err := s.schemalogStore.Insert(ctx, schema); err != nil {
			return fmt.Errorf("inserting schemalog entry for schema %q after schema snapshot: %w", schema, err)
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
	dumpRoles := make(map[string]role)
	alterTable := ""
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "SECURITY LABEL") &&
			isSecurityLabelForExcludedProvider(line, s.excludedSecurityLabels):
			// skip security labels if configured to do so for the specified providers
			continue
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
		case strings.HasPrefix(line, "ALTER TABLE") && strings.Contains(line, "REPLICA IDENTITY"):
			// REPLICA IDENTITY lines should be in the indicesAndConstraints section
			// since they reference constraints/indices that are also there
			indicesAndConstraints.WriteString(line)
			indicesAndConstraints.WriteString("\n\n")
		case strings.HasPrefix(line, "ALTER TABLE") && !strings.HasSuffix(line, ";"):
			// keep it in case the alter table is provided in two lines (pg_dump format)
			alterTable = line
		case strings.HasPrefix(line, "CREATE SEQUENCE"):
			qualifiedName, err := pglib.NewQualifiedName(strings.TrimPrefix(line, "CREATE SEQUENCE "))
			if err == nil {
				sequenceNames = append(sequenceNames, qualifiedName.String())
			}
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		case isRoleStatement(line):
			roles := s.roleSQLParser.extractRoleNamesFromLine(line)
			if hasExcludedRole(roles) {
				// if any of the roles is excluded or predefined, skip the whole line
				continue
			}

			for _, role := range roles {
				dumpRoles[role.name] = role
			}

			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")

		default:
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		}

	}

	return &dump{
		full:                  d,
		filtered:              []byte(filteredDump.String()),
		indicesAndConstraints: []byte(indicesAndConstraints.String()),
		sequences:             sequenceNames,
		roles:                 dumpRoles,
	}
}

func (s *SnapshotGenerator) filterRolesDump(rolesDump []byte, keepRoles map[string]role) []byte {
	scanner := bufio.NewScanner(bytes.NewReader(rolesDump))
	scanner.Split(bufio.ScanLines)
	var filteredDump strings.Builder

	skipLine := func(lineRoles []role) bool {
		for _, role := range lineRoles {
			_, roleFound := keepRoles[role.name]
			if !roleFound || isPredefinedRole(role.name) || isExcludedRole(role.name) {
				return true
			}
		}
		return false
	}

	for scanner.Scan() {
		line := scanner.Text()
		lineRoles := s.roleSQLParser.extractRoleNamesFromLine(line)
		if skipLine(lineRoles) {
			continue
		}

		// remove role attributes that require superuser privileges to be set
		// when the value is the same as the default.
		line = removeDefaultRoleAttributes(line)

		filteredDump.WriteString(line)
		filteredDump.WriteString("\n")
	}

	for _, role := range keepRoles {
		if isPredefinedRole(role.name) || isExcludedRole(role.name) || !role.isOwner {
			continue
		}
		// add a line to grant the role to the current user to avoid permission
		// issues when granting ownership (OWNER TO) when using non superuser
		// roles to restore the dump
		filteredDump.WriteString(fmt.Sprintf("GRANT %s TO CURRENT_USER;\n", pglib.QuoteIdentifier(role.name)))
	}

	return []byte(filteredDump.String())
}

type options interface {
	ToArgs() []string
}

func (s *SnapshotGenerator) dumpToFile(file string, opts options, d []byte) {
	if s.dumpDebugFile != "" {
		b := bytes.NewBufferString(fmt.Sprintf("pg_dump options: %v\n\n%s", opts.ToArgs(), string(d)))
		if err := os.WriteFile(file, b.Bytes(), 0o644); err != nil { //nolint:gosec
			s.logger.Error(err, fmt.Sprintf("writing dump to debug file %s", file))
		}
	}
}

func (s *SnapshotGenerator) sequenceDumpFile() string {
	return s.getDumpFileName("-sequences")
}

func (s *SnapshotGenerator) rolesDumpFile() string {
	return s.getDumpFileName("-roles")
}

func (s *SnapshotGenerator) getDumpFileName(suffix string) string {
	if s.dumpDebugFile == "" {
		return ""
	}

	fileExtension := filepath.Ext(s.dumpDebugFile)
	if fileExtension == "" {
		// if there's no extension, we assume it's a plain text file
		return s.dumpDebugFile + suffix
	}

	// if there's an extension, we append the suffix before the extension
	baseName := strings.TrimSuffix(s.dumpDebugFile, fileExtension)
	return baseName + suffix + fileExtension
}

func hasWildcardTable(tables []string) bool {
	return slices.Contains(tables, wildcard)
}

func hasWildcardSchema(schemaTables map[string][]string) bool {
	return schemaTables[wildcard] != nil
}

// returns all the lines of d1 that are not in d2
func getDumpsDiff(d1, d2 []byte) []byte {
	var diff strings.Builder
	lines1 := bytes.Split(d1, []byte("\n"))
	lines2 := bytes.Split(d2, []byte("\n"))
	lines2map := make(map[string]bool)
	for _, line := range lines2 {
		lines2map[string(line)] = true
	}

	for _, line := range lines1 {
		if !lines2map[string(line)] {
			diff.Write(line)
			diff.WriteString("\n")
		}
	}

	return []byte(diff.String())
}

func isSecurityLabelForExcludedProvider(line string, excludedProviders []string) bool {
	if slices.Contains(excludedProviders, wildcard) {
		return true
	}
	for _, provider := range excludedProviders {
		if strings.Contains(line, fmt.Sprintf("SECURITY LABEL FOR %s ", provider)) {
			return true
		}
	}
	return false
}
