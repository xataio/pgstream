// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"context"
	"fmt"
	"slices"

	pglib "github.com/xataio/pgstream/internal/postgres"
)

type optionGenerator struct {
	sourceURL              string
	targetURL              string
	cleanTargetDB          bool
	createTargetDB         bool
	includeGlobalDBObjects bool
	role                   string
	rolesSnapshotMode      string
	noOwner                bool
	noPrivileges           bool
	querier                pglib.Querier
}

const (
	roleSnapshotDisabled    = "disabled"
	roleSnapshotNoPasswords = "no_passwords"
)

func newOptionGenerator(querier pglib.Querier, cfg *Config) *optionGenerator {
	return &optionGenerator{
		sourceURL:              cfg.SourcePGURL,
		targetURL:              cfg.TargetPGURL,
		cleanTargetDB:          cfg.CleanTargetDB,
		createTargetDB:         cfg.CreateTargetDB,
		includeGlobalDBObjects: cfg.IncludeGlobalDBObjects,
		role:                   cfg.Role,
		rolesSnapshotMode:      cfg.RolesSnapshotMode,
		noOwner:                cfg.NoOwner,
		noPrivileges:           cfg.NoPrivileges,
		querier:                querier,
	}
}

func (o *optionGenerator) pgdumpSequenceDataOptions(sequences []string) *pglib.PGDumpOptions {
	if len(sequences) == 0 {
		return nil
	}
	return &pglib.PGDumpOptions{
		ConnectionString: o.sourceURL,
		Format:           "p",
		DataOnly:         true,
		Tables:           sequences,
	}
}

func (o *optionGenerator) pgdumpRolesOptions() *pglib.PGDumpAllOptions {
	if o.rolesSnapshotMode == roleSnapshotDisabled {
		return nil
	}

	return &pglib.PGDumpAllOptions{
		ConnectionString: o.sourceURL,
		RolesOnly:        true,
		Clean:            o.cleanTargetDB,
		Role:             o.role,
		NoOwner:          o.noOwner,
		NoPrivileges:     o.noPrivileges,
		NoPasswords:      o.rolesSnapshotMode == roleSnapshotNoPasswords,
	}
}

func (o *optionGenerator) pgrestoreOptions() pglib.PGRestoreOptions {
	return pglib.PGRestoreOptions{
		ConnectionString: o.targetURL,
		Clean:            o.cleanTargetDB,
		Format:           "p",
		Create:           o.createTargetDB,
	}
}

func (o *optionGenerator) pgdumpOptions(ctx context.Context, schemaTables map[string][]string, excludedTables map[string][]string) (*pglib.PGDumpOptions, error) {
	schemas := make([]string, 0, len(schemaTables))
	for schema := range schemaTables {
		schemas = append(schemas, quoteSchema(schema))
	}
	opts := &pglib.PGDumpOptions{
		ConnectionString: o.sourceURL,
		Format:           "p",
		SchemaOnly:       true,
		Schemas:          schemas,
		Clean:            o.cleanTargetDB,
		Create:           o.createTargetDB,
		NoOwner:          o.noOwner,
		NoPrivileges:     o.noPrivileges,
		Role:             o.role,
	}

	switch {
	case hasWildcardSchema(schemaTables):
		// no need to filter schemas, since we are including all of them
		opts.Schemas = nil
	case o.includeGlobalDBObjects:
		// instead of using the schema filter, we use the exclude schemas filter
		// to make sure extensions and other database global objects are
		// created. pg_dump will not include them when using the schema filter,
		// since they do not belong to the schema.
		var err error
		opts.ExcludeSchemas, err = o.pgdumpExcludedSchemas(ctx, schemas)
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
		opts.ExcludeTables, err = o.pgdumpExcludedTables(ctx, schema, tables)
		if err != nil {
			return nil, err
		}
	}

	for schema, tables := range excludedTables {
		if hasWildcardTable(tables) {
			opts.ExcludeSchemas = append(opts.ExcludeSchemas, pglib.QuoteIdentifier(schema))
			continue
		}
		for _, table := range tables {
			if !slices.Contains(opts.ExcludeTables, pglib.QuoteQualifiedIdentifier(schema, table)) {
				opts.ExcludeTables = append(opts.ExcludeTables, pglib.QuoteQualifiedIdentifier(schema, table))
			}
		}
	}

	return opts, nil
}

const (
	selectTablesQuery       = "SELECT schemaname,tablename FROM pg_tables WHERE tablename != ALL($1)"
	selectSchemaTablesQuery = "SELECT schemaname,tablename FROM pg_tables WHERE schemaname = $1 AND tablename != ALL($2)"
)

func (o *optionGenerator) pgdumpExcludedTables(ctx context.Context, schemaName string, includeTables []string) ([]string, error) {
	var query string
	var params []any

	// Make sure the schema and table names are unquoted when passing them as
	// parameters, since the system catalogs store unquoted names.
	unquotedIncludeTables := make([]string, len(includeTables))
	for i, table := range includeTables {
		unquotedIncludeTables[i] = pglib.UnquoteIdentifier(table)
	}

	switch schemaName {
	case wildcard:
		query = selectTablesQuery
		params = []any{unquotedIncludeTables}
	default:
		// If the schema is not wildcard, we need to filter by schema name.
		query = selectSchemaTablesQuery
		params = []any{pglib.UnquoteIdentifier(schemaName), unquotedIncludeTables}
	}

	// get all tables in the schema that are not in the include list
	rows, err := o.querier.Query(ctx, query, params...)
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

const selectSchemasQuery = "SELECT schema_name FROM information_schema.schemata WHERE schema_name != ALL($1)"

func (o *optionGenerator) pgdumpExcludedSchemas(ctx context.Context, includeSchemas []string) ([]string, error) {
	schemas := make([]string, 0, len(includeSchemas))
	for _, schema := range includeSchemas {
		// System catalogs store unquoted names, so we need to pass the schema
		// names without quotes as parameters.
		schemas = append(schemas, pglib.UnquoteIdentifier(schema))
	}

	// get all schemas in the database that are not in the snapshot request
	rows, err := o.querier.Query(ctx, selectSchemasQuery, schemas)
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

func quoteSchema(schema string) string {
	if schema == wildcard {
		return wildcard
	}
	return pglib.QuoteIdentifier(schema)
}
