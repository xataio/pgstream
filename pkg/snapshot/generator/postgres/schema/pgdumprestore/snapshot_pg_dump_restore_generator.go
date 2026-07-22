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
	"regexp"
	"slices"
	"strings"
	"sync"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibinstrumentation "github.com/xataio/pgstream/internal/postgres/instrumentation"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// SnapshotGenerator generates postgres schema snapshots using pg_dump and
// pg_restore
type SnapshotGenerator struct {
	sourceURL              string
	targetURL              string
	pgDumpFn               pglib.PGDumpFn
	pgDumpAllFn            pglib.PGDumpAllFn
	pgRestoreFn            pglib.PGRestoreFn
	sourceQuerier          pglib.Querier
	logger                 loglib.Logger
	generator              generator.SnapshotGenerator
	dumpDebugFile          string
	excludedSecurityLabels []string
	roleSQLParser          *roleSQLParser
	optionGenerator        *optionGenerator
	snapshotTracker        snapshotProgressTracker
	// restoreConflictTargetsBeforeData restores constraints/indexes that can
	// be used as INSERT ... ON CONFLICT targets before the wrapped data snapshot
	// generator runs. Other indexes and constraints, such as foreign keys, are
	// still restored after data is inserted.
	restoreConflictTargetsBeforeData bool
	// refreshMaterializedViews controls whether materialized views are refreshed
	// (REFRESH MATERIALIZED VIEW ... WITH DATA) after the table data has been
	// restored. Disabled by default since refreshing can be expensive on large
	// views.
	refreshMaterializedViews bool
	// indexConstraintSessionSettings are applied through PGOPTIONS only to
	// index and constraint restore sessions. They are cleared when restoring to
	// WAL (see WithRestoreToWAL), since that path converts the dump into WAL
	// events instead of running a psql/pg_restore session.
	indexConstraintSessionSettings []string
}

type snapshotProgressTracker interface {
	trackIndexesCreation(ctx context.Context)
	close() error
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
	// if set to true, materialized views will be refreshed (REFRESH MATERIALIZED
	// VIEW ... WITH DATA) after the table data has been restored. Disabled by
	// default since refreshing can be expensive on large views.
	RefreshMaterializedViews bool
	// Session settings in name=value format applied only while restoring indexes
	// and constraints. An empty list preserves the existing behavior.
	IndexConstraintSessionSettings []string
}

// sessionSettingRegex validates a single index/constraint session setting of
// the form name=value. The name must be a valid PostgreSQL configuration
// parameter identifier, and neither the name nor the value may contain
// whitespace: the settings are passed to the restore subprocess through
// PGOPTIONS, which libpq splits on whitespace, so allowing whitespace would let
// a single setting inject additional backend command-line options.
var sessionSettingRegex = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.]*=\S+$`)

var errInvalidSessionSetting = errors.New("invalid index constraint session setting, must be a whitespace-free name=value pair")

func validateSessionSettings(settings []string) error {
	for _, setting := range settings {
		if !sessionSettingRegex.MatchString(setting) {
			return fmt.Errorf("%w: %q", errInvalidSessionSetting, setting)
		}
	}
	return nil
}

type Option func(s *SnapshotGenerator)

type dump struct {
	full                  []byte
	filtered              []byte
	cleanupPart           []byte
	indicesAndConstraints []byte
	// views holds the statements that are validated against the catalog at
	// creation time and can rely on indices and constraints existing (views,
	// materialized views, _RETURN rules and functions with SQL-standard
	// bodies), so they are restored after the indices and constraints.
	views                     []byte
	materializedViewRefreshes []byte
	sequences                 []string
	roles                     map[string]role
	eventTriggers             []byte
}

const (
	publicSchema = "public"
	wildcard     = "*"
)

// NewSnapshotGenerator will return a postgres schema snapshot generator that
// uses pg_dump and pg_restore to sync the schema of two postgres databases
func NewSnapshotGenerator(ctx context.Context, c *Config, opts ...Option) (*SnapshotGenerator, error) {
	if err := validateSessionSettings(c.IndexConstraintSessionSettings); err != nil {
		return nil, err
	}

	sourceConnPool, err := pglib.NewConnPool(ctx, c.SourcePGURL)
	if err != nil {
		return nil, err
	}

	sg := &SnapshotGenerator{
		sourceURL:                      c.SourcePGURL,
		targetURL:                      c.TargetPGURL,
		pgDumpFn:                       pglib.RunPGDump,
		pgDumpAllFn:                    pglib.RunPGDumpAll,
		pgRestoreFn:                    pglib.RunPGRestore,
		logger:                         loglib.NewNoopLogger(),
		dumpDebugFile:                  c.DumpDebugFile,
		excludedSecurityLabels:         c.ExcludedSecurityLabels,
		roleSQLParser:                  &roleSQLParser{},
		sourceQuerier:                  sourceConnPool,
		optionGenerator:                newOptionGenerator(sourceConnPool, c),
		refreshMaterializedViews:       c.RefreshMaterializedViews,
		indexConstraintSessionSettings: c.IndexConstraintSessionSettings,
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
		sg.sourceQuerier, err = pglibinstrumentation.NewQuerier(sg.sourceQuerier, i)
		if err != nil {
			// this should never happen
			panic(err)
		}

		sg.pgDumpFn = pglibinstrumentation.NewPGDumpFn(sg.pgDumpFn, i)
		sg.pgDumpAllFn = pglibinstrumentation.NewPGDumpAllFn(sg.pgDumpAllFn, i)
		sg.pgRestoreFn = pglibinstrumentation.NewPGRestoreFn(sg.pgRestoreFn, i)
	}
}

func WithProgressTracking(ctx context.Context) Option {
	return func(sg *SnapshotGenerator) {
		snapshotTracker, err := newSnapshotTracker(ctx, sg.targetURL)
		if err != nil {
			sg.logger.Error(err, "creating snapshot tracker")
			return
		}
		sg.snapshotTracker = snapshotTracker
	}
}

func WithRestoreToWAL(processor processor.Processor) Option {
	return func(sg *SnapshotGenerator) {
		sg.pgRestoreFn = newPGSnapshotWALRestore(processor, sg.sourceQuerier).restoreToWAL
		sg.indexConstraintSessionSettings = nil
	}
}

// WithRestoreConflictTargetsBeforeData restores constraints and indexes that
// can be used as INSERT ... ON CONFLICT targets (primary keys, unique
// constraints and unique indexes) before the wrapped data snapshot generator
// runs. This is required when the data writer emits
// INSERT ... ON CONFLICT DO UPDATE, since the target table must already have a
// matching unique or primary key constraint at insert time.
func WithRestoreConflictTargetsBeforeData() Option {
	return func(sg *SnapshotGenerator) {
		sg.restoreConflictTargetsBeforeData = true
	}
}

func (s *SnapshotGenerator) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) (err error) {
	s.logger.Info("creating schema snapshot", loglib.Fields{"schemaTables": ss.SchemaTables, "schemaOnlyTables": ss.SchemaOnlyTables})

	// make sure any empty schemas are filtered out
	dataSchemas := make(map[string][]string, len(ss.SchemaTables))
	for schema, tables := range ss.SchemaTables {
		if len(tables) > 0 {
			dataSchemas[schema] = tables
		}
	}
	schemaOnly := make(map[string][]string, len(ss.SchemaOnlyTables))
	for schema, tables := range ss.SchemaOnlyTables {
		if len(tables) > 0 {
			schemaOnly[schema] = tables
		}
	}
	if err := pglib.ValidateWildcardSchemaTables(schemaOnly); err != nil {
		return err
	}
	// the schema snapshot scope is the union of the data snapshot tables and
	// the schema-only tables
	dumpSchemas := make(map[string][]string, len(dataSchemas)+len(schemaOnly))
	for schema, tables := range dataSchemas {
		dumpSchemas[schema] = tables
	}
	for schema, tables := range schemaOnly {
		dumpSchemas[schema] = mergeTables(dumpSchemas[schema], tables)
	}
	// nothing to dump
	if len(dumpSchemas) == 0 {
		return nil
	}

	// DUMP

	dump, err := s.dumpSchema(ctx, dataSchemas, schemaOnly, ss.SchemaExcludedTables)
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

	// RESTORE

	if err := s.restoreSchemas(ctx, dumpSchemas); err != nil {
		return err
	}

	if s.optionGenerator.cleanTargetDB {
		s.logger.Info("restoring cleanup")
		if err := s.restoreDump(ctx, dump.cleanupPart); err != nil {
			return err
		}
	}

	if rolesDump != nil {
		s.logger.Info("restoring roles")
		if err := s.restoreDump(ctx, rolesDump); err != nil {
			return err
		}
	}

	s.logger.Info("restoring schema")
	if err := s.restoreDump(ctx, dump.filtered); err != nil {
		return err
	}

	indicesAndConstraintsDump := dump.indicesAndConstraints
	if s.generator != nil && s.restoreConflictTargetsBeforeData {
		conflictTargets, remaining := splitConflictTargetConstraints(dump.indicesAndConstraints)
		if err := s.restoreIndicesAndConstraints(ctx, conflictTargets, ss); err != nil {
			return err
		}
		indicesAndConstraintsDump = remaining
	}

	// call the wrapped snapshot generator if any before restoring sequences,
	// indices and constraints to improve performance. When the data snapshot
	// writer emits INSERT ... ON CONFLICT DO UPDATE, the subset of constraints
	// needed as conflict targets is restored before data above.
	if s.generator != nil {
		if err := s.generator.CreateSnapshot(ctx, ss); err != nil {
			return err
		}
	}

	// apply the sequences, indices and constraints when the wrapped generator has finished
	s.logger.Info("restoring sequence data", loglib.Fields{"schemaTables": ss.SchemaTables})
	if err := s.restoreDump(ctx, sequenceDump); err != nil {
		return err
	}

	if err := s.restoreIndicesAndConstraints(ctx, indicesAndConstraintsDump, ss); err != nil {
		return err
	}

	s.logger.Info("restoring views")
	if err := s.restoreDump(ctx, dump.views); err != nil {
		return err
	}

	if !s.refreshMaterializedViews {
		return nil
	}

	s.logger.Info("refreshing materialized views")
	return s.restoreDump(ctx, dump.materializedViewRefreshes)
}

func splitConflictTargetConstraints(d []byte) ([]byte, []byte) {
	blocks := strings.Split(string(d), "\n\n")
	connectBlocks := []string{}
	conflictTargetBlocks := []string{}
	remainingBlocks := []string{}

	for _, block := range blocks {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		switch {
		case strings.Contains(block, `\connect`):
			connectBlocks = append(connectBlocks, block)
		case isConflictTargetConstraint(block):
			conflictTargetBlocks = append(conflictTargetBlocks, block)
		default:
			remainingBlocks = append(remainingBlocks, block)
		}
	}

	return joinDumpBlocks(connectBlocks, conflictTargetBlocks), joinDumpBlocks(connectBlocks, remainingBlocks)
}

func joinDumpBlocks(connectBlocks, blocks []string) []byte {
	if len(blocks) == 0 {
		return nil
	}
	allBlocks := make([]string, 0, len(connectBlocks)+len(blocks))
	allBlocks = append(allBlocks, connectBlocks...)
	allBlocks = append(allBlocks, blocks...)
	return []byte(strings.Join(allBlocks, "\n\n") + "\n\n")
}

func isConflictTargetConstraint(block string) bool {
	upperBlock := strings.ToUpper(block)
	if strings.HasPrefix(upperBlock, "CREATE UNIQUE INDEX") {
		return true
	}
	if !strings.Contains(upperBlock, "ADD CONSTRAINT") {
		return false
	}
	return strings.Contains(upperBlock, " PRIMARY KEY (") ||
		strings.Contains(upperBlock, " PRIMARY KEY USING INDEX ") ||
		strings.Contains(upperBlock, " UNIQUE (") ||
		strings.Contains(upperBlock, " UNIQUE NULLS NOT DISTINCT (") ||
		strings.Contains(upperBlock, " UNIQUE USING INDEX ")
}

func (s *SnapshotGenerator) restoreIndicesAndConstraints(ctx context.Context, dump []byte, ss *snapshot.Snapshot) error {
	s.logger.Info("restoring schema indices and constraints", loglib.Fields{"schemaTables": ss.SchemaTables})
	opts := s.optionGenerator.pgrestoreOptions()
	opts.SessionSettings = s.indexConstraintSessionSettings
	if s.snapshotTracker != nil {
		return s.restoreIndicesWithTracking(ctx, opts, dump)
	}
	return s.restoreDumpWithOptions(ctx, opts, dump)
}

func (s *SnapshotGenerator) Close() error {
	if s.generator != nil {
		if err := s.generator.Close(); err != nil {
			s.logger.Error(err, "closing data snapshot generator")
		}
	}

	if s.snapshotTracker != nil {
		if err := s.snapshotTracker.close(); err != nil {
			s.logger.Error(err, "closing snapshot tracker")
		}
	}

	if s.sourceQuerier != nil {
		if err := s.sourceQuerier.Close(context.Background()); err != nil {
			s.logger.Error(err, "closing source querier")
		}
	}

	return nil
}

func (s *SnapshotGenerator) dumpSchema(ctx context.Context, schemaTables, schemaOnlyTables, excludedTables map[string][]string) (*dump, error) {
	pgdumpOpts, err := s.optionGenerator.pgdumpOptions(ctx, schemaTables, schemaOnlyTables, excludedTables)
	if err != nil {
		return nil, fmt.Errorf("preparing pg_dump options: %w", err)
	}

	// produce first the schema dump without the clean up statements
	pgdumpOpts.Clean = false

	s.logger.Debug("dumping schema", loglib.Fields{"pg_dump_options": pgdumpOpts.ToArgs(), "schema_tables": schemaTables})
	d, err := s.pgDumpFn(ctx, *pgdumpOpts)
	defer s.dumpToFile(s.dumpDebugFile, pgdumpOpts, d)
	if err != nil {
		s.logger.Error(err, "pg_dump for schema failed", loglib.Fields{"pgdumpOptions": pgdumpOpts.ToArgs()})
		return nil, fmt.Errorf("dumping schema: %w", err)
	}

	parsedDump := s.parseDump(d)
	// remove the event triggers that reference functions from excluded schemas
	parsedDump.filtered = append(parsedDump.filtered, s.filterTriggers(parsedDump.eventTriggers, pgdumpOpts.ExcludeSchemas)...)

	s.dumpToFile(s.getDumpFileName("-filtered"), pgdumpOpts, parsedDump.filtered)
	s.dumpToFile(s.getDumpFileName("-indices-constraints"), pgdumpOpts, parsedDump.indicesAndConstraints)
	s.dumpToFile(s.getDumpFileName("-views"), pgdumpOpts, parsedDump.views)

	// only if clean is enabled, produce the clean up part of the dump
	if s.optionGenerator.cleanTargetDB {
		// In case clean is enabled, we need the cleanup part of the dump separately, which will be restored before the roles dump.
		// This will allow us to drop the roles safely, without getting dependency errors.
		pgdumpOpts.Clean = true
		s.logger.Debug("dumping schema clean up", loglib.Fields{"pg_dump_options": pgdumpOpts.ToArgs(), "schema_tables": schemaTables})
		dumpWithCleanUp, err := s.pgDumpFn(ctx, *pgdumpOpts)
		if err != nil {
			s.logger.Error(err, "pg_dump for schema failed", loglib.Fields{"pgdumpOptions": pgdumpOpts.ToArgs()})
			return nil, fmt.Errorf("dumping schema: %w", err)
		}
		parsedDump.cleanupPart = getDumpsDiff(dumpWithCleanUp, d)
		s.dumpToFile(s.getDumpFileName("-cleanup"), pgdumpOpts, parsedDump.cleanupPart)
	}

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

// if we use table filtering in the pg_dump command, the schema creation will
// not be dumped, so it needs to be created explicitly (except for public
// schema)
func (s *SnapshotGenerator) restoreSchemas(ctx context.Context, schemaTables map[string][]string) error {
	schemaDump := strings.Builder{}
	for schema, tables := range schemaTables {
		if len(tables) > 0 && schema != publicSchema && schema != wildcard {
			fmt.Fprintf(&schemaDump, "CREATE SCHEMA IF NOT EXISTS %s;\n", pglib.QuoteIdentifier(schema))
		}
	}

	return s.restoreDump(ctx, []byte(schemaDump.String()))
}

func (s *SnapshotGenerator) restoreDump(ctx context.Context, dump []byte) error {
	return s.restoreDumpWithOptions(ctx, s.optionGenerator.pgrestoreOptions(), dump)
}

func (s *SnapshotGenerator) restoreDumpWithOptions(ctx context.Context, opts pglib.PGRestoreOptions, dump []byte) error {
	if len(dump) == 0 {
		return nil
	}

	_, err := s.pgRestoreFn(ctx, opts, dump)
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

func (s *SnapshotGenerator) parseDump(d []byte) *dump {
	scanner := bufio.NewScanner(bytes.NewReader(d))
	scanner.Split(bufio.ScanLines)
	indicesAndConstraints := strings.Builder{}
	filteredDump := strings.Builder{}
	eventTriggersDump := strings.Builder{}
	viewsDump := strings.Builder{}
	sequenceNames := []string{}
	dumpRoles := make(map[string]role)
	connectStatements := []string{}
	materializedViews := []string{}
	alterTable := ""
	createEventTrigger := ""
	createView := []string{}
	createViewIsMaterialized := false
	createRule := []string{}
	functionParser := &sqlFunctionParser{}
	materializedViewNames := map[string]struct{}{}
	skipLegacyPLPGSQLHandlerFunction := false
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case skipLegacyPLPGSQLHandlerFunction:
			if strings.HasSuffix(line, ";") {
				skipLegacyPLPGSQLHandlerFunction = false
			}
			continue
		case isLegacyPLPGSQLHandlerFunctionStart(line):
			skipLegacyPLPGSQLHandlerFunction = !strings.HasSuffix(line, ";")
			continue
		case functionParser.inProgress || isSQLFunctionStart(line):
			if statement, deferred, done := functionParser.addLine(line); done {
				if deferred {
					// functions with SQL-standard bodies (BEGIN ATOMIC/RETURN)
					// are parsed and validated at creation time, so they can
					// rely on indices and constraints existing (e.g. GROUP BY
					// primary key functional dependency) and must be restored
					// after them, along with the views
					viewsDump.WriteString(strings.Join(statement, "\n"))
					viewsDump.WriteString("\n\n")
				} else {
					filteredDump.WriteString(strings.Join(statement, "\n"))
					filteredDump.WriteString("\n")
				}
			}
		case strings.HasPrefix(line, "SECURITY LABEL") &&
			isSecurityLabelForExcludedProvider(line, s.excludedSecurityLabels):
			// skip security labels if configured to do so for the specified providers
			continue
		case alterTable != "":
			// check if the previous alter table line is split in two lines and matches a constraint
			if strings.Contains(line, "ADD CONSTRAINT") || isClusterOnAlterTable(line) {
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
		case strings.HasPrefix(line, "CREATE EVENT TRIGGER"):
			createEventTrigger = line
			fallthrough
		case createEventTrigger != "":
			// check if the previous create event trigger line is split in multiple lines
			if strings.HasSuffix(line, ";") {
				eventTriggersDump.WriteString(line)
				eventTriggersDump.WriteString("\n\n")
				createEventTrigger = ""
				continue
			}
			eventTriggersDump.WriteString(line)
			eventTriggersDump.WriteString("\n")

		case len(createView) > 0 || isViewStatementStart(line):
			if len(createView) == 0 {
				if name, ok := materializedViewName(line); ok {
					materializedViewNames[name] = struct{}{}
					materializedViews = append(materializedViews, name)
				}
				createViewIsMaterialized = strings.HasPrefix(line, "CREATE MATERIALIZED VIEW")
			}
			createView = append(createView, line)
			if strings.HasSuffix(line, ";") {
				statement := strings.Join(createView, "\n")
				if !createViewIsMaterialized && isDummyViewStatement(createView) {
					// pg_dump breaks circular view dependencies by emitting a
					// dummy view (a bare SELECT of NULL casts with no FROM
					// clause) upfront and the real definition (CREATE OR
					// REPLACE VIEW) at the end of the dump. The dummy must be
					// restored early so that objects in the main schema dump
					// referencing the view row type (e.g. functions) can be
					// created; it cannot depend on indices or constraints.
					filteredDump.WriteString(statement)
					filteredDump.WriteString("\n")
				} else {
					viewsDump.WriteString(statement)
					viewsDump.WriteString("\n\n")
				}
				createView = []string{}
			}
		case len(createRule) > 0 || strings.HasPrefix(line, `CREATE RULE "_RETURN" AS`):
			// older pg_dump versions break circular view dependencies with a
			// dummy table converted into a view by a _RETURN rule; the rule
			// body is validated at creation time, so it is restored along
			// with the views, after indices and constraints
			createRule = append(createRule, line)
			if strings.HasSuffix(line, ";") {
				viewsDump.WriteString(strings.Join(createRule, "\n"))
				viewsDump.WriteString("\n\n")
				createRule = []string{}
			}

		case strings.Contains(line, `\connect`):
			connectStatements = append(connectStatements, line)
			indicesAndConstraints.WriteString(line)
			indicesAndConstraints.WriteString("\n\n")
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
			viewsDump.WriteString(line)
			viewsDump.WriteString("\n\n")
		case isIndexStatement(line) && isIndexOnMaterializedView(line, materializedViewNames):
			viewsDump.WriteString(line)
			viewsDump.WriteString("\n\n")
		case isIndexStatement(line),
			strings.HasPrefix(line, "CREATE CONSTRAINT"),
			strings.HasPrefix(line, "CREATE TRIGGER"),
			strings.HasPrefix(line, "COMMENT ON CONSTRAINT"),
			strings.HasPrefix(line, "COMMENT ON INDEX"),
			strings.HasPrefix(line, "COMMENT ON TRIGGER"):
			indicesAndConstraints.WriteString(line)
			indicesAndConstraints.WriteString("\n\n")
		case strings.HasPrefix(line, "ALTER TABLE") && strings.Contains(line, "ADD CONSTRAINT"):
			indicesAndConstraints.WriteString(line)
		case strings.HasPrefix(line, "ALTER TABLE") && isClusterOnAlterTable(line):
			indicesAndConstraints.WriteString(line)
			indicesAndConstraints.WriteString("\n\n")
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
				if role.isOwner() {
					// Add lines to grant access to roles that have object
					// ownership in the schema, otherwise restoring will fail
					// with permission denied for schema. This must be done
					// before the ALTER OWNER TO statements. This needs to be
					// done here, once the schema being referenced exists.
					//
					// Cleanup handling is not required, since the schema will
					// be dropped anyway if clean is enabled.
					for schema := range role.schemasWithOwnership {
						fmt.Fprintf(&filteredDump, "GRANT ALL ON SCHEMA %s TO %s;\n", pglib.QuoteIdentifier(schema), pglib.QuoteIdentifier(role.name))
					}
				}
			}

			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")

		default:
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		}

	}

	return &dump{
		full:                      d,
		filtered:                  []byte(filteredDump.String()),
		indicesAndConstraints:     []byte(indicesAndConstraints.String()),
		views:                     []byte(viewsDump.String()),
		materializedViewRefreshes: materializedViewRefreshDump(connectStatements, materializedViews),
		sequences:                 sequenceNames,
		roles:                     dumpRoles,
		eventTriggers:             []byte(eventTriggersDump.String()),
	}
}

func isClusterOnAlterTable(line string) bool {
	return strings.Contains(line, " CLUSTER ON ")
}

func isViewStatementStart(line string) bool {
	return strings.HasPrefix(line, "CREATE VIEW ") ||
		strings.HasPrefix(line, "CREATE OR REPLACE VIEW ") ||
		strings.HasPrefix(line, "CREATE MATERIALIZED VIEW ")
}

// isDummyViewStatement returns true if the view statement matches the
// placeholder pg_dump emits to break circular view dependencies: a bare
// SELECT of NULL-cast columns with no FROM clause, e.g.
//
//	CREATE VIEW public.v AS
//	SELECT
//	    NULL::integer AS id,
//	    NULL::text AS name;
func isDummyViewStatement(lines []string) bool {
	if len(lines) < 3 || strings.TrimSpace(lines[1]) != "SELECT" {
		return false
	}
	for _, line := range lines[2:] {
		column := strings.TrimSpace(strings.TrimRight(line, ";,"))
		if !strings.HasPrefix(column, "NULL::") {
			return false
		}
	}
	return true
}

func isSQLFunctionStart(line string) bool {
	return strings.HasPrefix(line, "CREATE FUNCTION ") ||
		strings.HasPrefix(line, "CREATE OR REPLACE FUNCTION ") ||
		strings.HasPrefix(line, "CREATE PROCEDURE ") ||
		strings.HasPrefix(line, "CREATE OR REPLACE PROCEDURE ")
}

// sqlFunctionParser buffers the beginning of a CREATE FUNCTION/PROCEDURE
// statement until pg_dump's body introduction reveals its style:
//
//   - `AS ...` (classic string/dollar quoted body): the body is only parsed
//     on execution, so the statement can be restored in place. The parser
//     stops and hands the buffered lines back; the rest of the statement
//     doesn't need buffering and flows through the regular dump parsing,
//     unchanged.
//   - `BEGIN ATOMIC <statements> END;` or `RETURN <expression>;`
//     (SQL-standard body): the body is parsed and validated at creation time,
//     so it can rely on indices and constraints existing (e.g. GROUP BY
//     primary key functional dependency) and must be deferred. These bodies
//     are deparsed plain SQL with no quoting, so the statement end can be
//     detected without tracking dollar quotes.
//
// The lines between the header and the body introduction are attribute lines
// (LANGUAGE, IMMUTABLE, SET, COST, ...) and cannot contain quoted bodies.
type sqlFunctionParser struct {
	inProgress bool
	lines      []string
	atomicBody bool
	returnBody bool
}

// addLine buffers the line and reports whether the parser is finished with
// the statement. Once done, statement holds the buffered lines and deferred
// indicates whether they are a SQL-standard body function that needs to be
// restored after indices and constraints.
func (p *sqlFunctionParser) addLine(line string) (statement []string, deferred, done bool) {
	p.inProgress = true
	p.lines = append(p.lines, line)
	trimmedLine := strings.TrimSpace(line)
	switch {
	case p.atomicBody:
		// pg_dump places the closing END of the atomic body on its own line
		if trimmedLine == "END;" {
			return p.finish(true)
		}
	case p.returnBody:
		if strings.HasSuffix(line, ";") {
			return p.finish(true)
		}
	case trimmedLine == "BEGIN ATOMIC":
		p.atomicBody = true
	case strings.HasPrefix(trimmedLine, "RETURN ") || strings.HasPrefix(trimmedLine, "RETURN("):
		// the RETURNS clause of the header cannot match, since the first line
		// of the statement always starts with CREATE
		if strings.HasSuffix(line, ";") {
			return p.finish(true)
		}
		p.returnBody = true
	case strings.HasPrefix(trimmedLine, "AS "),
		strings.HasSuffix(line, ";"):
		// classic quoted body or end of statement without a recognized
		// SQL-standard body: restore in place
		return p.finish(false)
	}
	return nil, false, false
}

func (p *sqlFunctionParser) finish(deferred bool) ([]string, bool, bool) {
	statement := p.lines
	*p = sqlFunctionParser{}
	return statement, deferred, true
}

func isLegacyPLPGSQLHandlerFunctionStart(line string) bool {
	return strings.HasPrefix(line, "CREATE FUNCTION public.plpgsql_call_handler() RETURNS language_handler") ||
		strings.HasPrefix(line, "CREATE FUNCTION public.plpgsql_validator(oid) RETURNS void")
}

func materializedViewRefreshDump(connectStatements, materializedViews []string) []byte {
	if len(materializedViews) == 0 {
		return nil
	}

	refreshDump := strings.Builder{}
	for _, statement := range connectStatements {
		refreshDump.WriteString(statement)
		refreshDump.WriteString("\n\n")
	}
	for _, view := range materializedViews {
		fmt.Fprintf(&refreshDump, "REFRESH MATERIALIZED VIEW %s WITH DATA;\n\n", view)
	}
	return []byte(refreshDump.String())
}

func isIndexStatement(line string) bool {
	return strings.HasPrefix(line, "CREATE INDEX") ||
		strings.HasPrefix(line, "CREATE UNIQUE INDEX")
}

func materializedViewName(line string) (string, bool) {
	if !strings.HasPrefix(line, "CREATE MATERIALIZED VIEW ") {
		return "", false
	}

	name := strings.TrimPrefix(line, "CREATE MATERIALIZED VIEW ")
	if idx := strings.Index(name, " AS"); idx >= 0 {
		return strings.TrimSpace(name[:idx]), true
	}
	return "", false
}

func isIndexOnMaterializedView(line string, materializedViewNames map[string]struct{}) bool {
	if len(materializedViewNames) == 0 {
		return false
	}

	onIdx := strings.Index(line, " ON ")
	if onIdx < 0 {
		return false
	}
	rest := line[onIdx+len(" ON "):]
	usingIdx := strings.Index(rest, " USING ")
	if usingIdx < 0 {
		return false
	}

	_, ok := materializedViewNames[strings.TrimSpace(rest[:usingIdx])]
	return ok
}

func (s *SnapshotGenerator) filterTriggers(eventTriggersDump []byte, excludedSchemas []string) []byte {
	if len(excludedSchemas) == 0 {
		return eventTriggersDump
	}

	scanner := bufio.NewScanner(bytes.NewReader(eventTriggersDump))
	scanner.Split(bufio.ScanLines)
	var filteredDump strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		// get the schema name from the event trigger definition after the EXECUTE FUNCTION clause
		triggerSchema, err := extractEventTriggerSchema(line)
		if err != nil || slices.Contains(excludedSchemas, triggerSchema) {
			continue
		}

		// schema not excluded
		filteredDump.WriteString(line)
		filteredDump.WriteString("\n")
	}

	return []byte(filteredDump.String())
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
		line = s.removeRestrictedRoleAttributes(line)

		filteredDump.WriteString(line)
		filteredDump.WriteString("\n")
	}

	for _, role := range keepRoles {
		if isPredefinedRole(role.name) || isExcludedRole(role.name) || !role.isOwner() {
			continue
		}
		// add a line to grant the role to the current user to avoid permission
		// issues when granting ownership (OWNER TO) when using non superuser
		// roles to restore the dump
		fmt.Fprintf(&filteredDump, "GRANT %s TO CURRENT_USER;\n", pglib.QuoteIdentifier(role.name))
	}

	return []byte(filteredDump.String())
}

func (s *SnapshotGenerator) removeRestrictedRoleAttributes(line string) string {
	if !strings.Contains(line, "REPLICATION") || !s.isAWSTarget() {
		return line
	}
	// in AWS RDS, the REPLICATION attribute is restricted to rds_replication
	// role so we need to remove it from the role creation line to avoid errors
	// and add a line with the GRANT statement
	line = strings.ReplaceAll(line, " REPLICATION", "")
	roles := s.roleSQLParser.extractRoleNamesFromLine(line)
	if len(roles) == 0 {
		return line
	}
	line += fmt.Sprintf("\nGRANT rds_replication TO %s;", pglib.QuoteIdentifier(roles[0].name))
	return line
}

func (s *SnapshotGenerator) isAWSTarget() bool {
	return strings.Contains(s.targetURL, "rds.amazonaws.com")
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

func (s *SnapshotGenerator) restoreIndicesWithTracking(ctx context.Context, opts pglib.PGRestoreOptions, dump []byte) error {
	wg := sync.WaitGroup{}
	wg.Add(1)
	trackingCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		defer wg.Done()
		s.snapshotTracker.trackIndexesCreation(trackingCtx)
	}()
	err := s.restoreDumpWithOptions(ctx, opts, dump)
	// wait for the tracking to finish once the restore is done
	cancel()
	wg.Wait()
	return err
}

func hasWildcardTable(tables []string) bool {
	return slices.Contains(tables, wildcard)
}

// mergeTables returns the deduplicated union of both table lists, collapsing
// to the wildcard if either list contains it.
func mergeTables(t1, t2 []string) []string {
	if hasWildcardTable(t1) || hasWildcardTable(t2) {
		return []string{wildcard}
	}
	merged := slices.Clone(t1)
	for _, table := range t2 {
		if !slices.Contains(merged, table) {
			merged = append(merged, table)
		}
	}
	return merged
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

func extractEventTriggerSchema(line string) (string, error) {
	// example line:
	// CREATE EVENT TRIGGER my_trigger ON sql_drop WHEN TAG IN ('DROP_INDEX') EXECUTE FUNCTION schema.my_function();
	parts := strings.Split(line, "EXECUTE FUNCTION")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid event trigger line: %s", line)
	}
	functionPart := strings.TrimSpace(parts[1])
	functionParts := strings.Split(functionPart, ".")
	if len(functionParts) != 2 {
		return "", fmt.Errorf("invalid function part in event trigger line: %s", line)
	}

	return pglib.QuoteIdentifier(functionParts[0]), nil
}
