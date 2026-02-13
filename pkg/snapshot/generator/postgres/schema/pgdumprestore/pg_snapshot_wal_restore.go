// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	jsonlib "github.com/xataio/pgstream/internal/json"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// PGSnapshotWALRestore handles restoring a pg_dump snapshot as WAL DDL events
type PGSnapshotWALRestore struct {
	processor processor.Processor
	querier   pglib.Querier
}

var ddlCommandTags = []string{
	"CREATE TABLE",
	"CREATE UNIQUE INDEX",
	"CREATE INDEX",
	"CREATE CONSTRAINT",
	"CREATE SEQUENCE",
	"CREATE MATERIALIZED VIEW",
	"CREATE VIEW",
	"CREATE FUNCTION",
	"CREATE PROCEDURE",
	"CREATE TRIGGER",
	"CREATE TYPE",
	"CREATE SCHEMA",
	"CREATE EXTENSION",
	"ALTER TABLE",
	"ALTER SEQUENCE",
	"ALTER MATERIALIZED VIEW",
	"ALTER VIEW",
	"ALTER FUNCTION",
	"ALTER PROCEDURE",
	"ALTER TYPE",
	"ALTER SCHEMA",
	"DROP TABLE",
	"DROP INDEX",
	"DROP SEQUENCE",
	"DROP MATERIALIZED VIEW",
	"DROP VIEW",
	"DROP FUNCTION",
	"DROP PROCEDURE",
	"DROP TRIGGER",
	"DROP TYPE",
	"DROP SCHEMA",
	"DROP EXTENSION",
	"COMMENT ON CONSTRAINT",
	"COMMENT ON INDEX",
	"COMMENT ON TABLE",
	"COMMENT ON COLUMN",
	"COMMENT ON",
	"GRANT",
	"REVOKE",
}

func newPGSnapshotWALRestore(processor processor.Processor, querier pglib.Querier) *PGSnapshotWALRestore {
	return &PGSnapshotWALRestore{
		processor: processor,
		querier:   querier,
	}
}

// restoreToWAL restores the given dump as WAL DDL messages to be processed by the processor
func (r *PGSnapshotWALRestore) restoreToWAL(ctx context.Context, _ pglib.PGRestoreOptions, dump []byte) (string, error) {
	scanner := bufio.NewScanner(bytes.NewReader(dump))
	scanner.Split(bufio.ScanLines)

	var currentStatement strings.Builder
	var inDollarQuote bool
	var dollarQuoteTag string
	timestamp := time.Now().UTC().Format(time.RFC3339)

	for scanner.Scan() {
		line := scanner.Text()

		// Skip empty lines and comments when not in a statement
		if currentStatement.Len() == 0 {
			trimmedLine := strings.TrimSpace(line)
			if trimmedLine == "" || strings.HasPrefix(trimmedLine, "--") {
				continue
			}
		}

		// Add the line to the current statement
		if currentStatement.Len() > 0 {
			currentStatement.WriteString("\n")
		}
		currentStatement.WriteString(line)

		// Track dollar-quoted strings
		inDollarQuote, dollarQuoteTag = updateDollarQuoteState(line, inDollarQuote, dollarQuoteTag)

		// Check if the statement is complete (ends with semicolon outside of dollar quotes)
		trimmedLine := strings.TrimSpace(line)
		if !inDollarQuote && strings.HasSuffix(trimmedLine, ";") {
			statement := currentStatement.String()
			trimmedStatement := strings.TrimSpace(statement)

			// Check if this is a DDL statement
			if isDDLStatement(trimmedStatement) {
				if err := r.processDDLStatement(ctx, trimmedStatement, timestamp); err != nil {
					return "", fmt.Errorf("processing DDL statement: %w", err)
				}
			}

			// Reset for the next statement
			currentStatement.Reset()
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scanning dump: %w", err)
	}

	return "", nil
}

// updateDollarQuoteState tracks whether we're inside a dollar-quoted string
// and returns the updated state and the current dollar quote tag.
// Uses extractDollarQuoteTag for spec-compliant tag detection (rejects $1$, $5$ etc.).
func updateDollarQuoteState(line string, inDollarQuote bool, currentTag string) (bool, string) {
	if inDollarQuote {
		// Look for the closing tag
		if strings.Contains(line, currentTag) {
			inDollarQuote = false
			currentTag = ""
		}
		return inDollarQuote, currentTag
	}

	// Not currently in a dollar quote — check if one opens on this line
	if tag := extractDollarQuoteTag(line); tag != "" {
		if strings.Count(line, tag) >= 2 {
			// Opens and closes on the same line (e.g. AS $$ BEGIN ... END; $$;)
			return false, ""
		}
		return true, tag
	}

	return false, ""
}

// processDDLStatement creates a WAL event for a DDL statement and processes it
func (r *PGSnapshotWALRestore) processDDLStatement(ctx context.Context, statement, timestamp string) error {
	// Extract command tag from the statement
	commandTag := extractCommandTag(statement)

	// Extract schema and object names from the statement
	schemaName, objectNames := extractSchemaAndObjects(statement, commandTag)

	// Fetch objects metadata from the database if querier is available
	objects := []wal.DDLObject{}
	if r.querier != nil && len(objectNames) > 0 {
		for _, objName := range objectNames {
			// Parse schema.table from object name
			schema, table := parseSchemaTable(objName, schemaName)

			// Query for table metadata
			obj, err := r.getTableObject(ctx, schema, table)
			if err != nil {
				// If table not found, it might have been dropped or not yet created
				// For DROP statements, we can't query metadata, so we add minimal info
				if !errors.Is(err, pglib.ErrNoRows) {
					return fmt.Errorf("getting table object %s.%s: %w", schema, table, err)
				}
				// Add minimal object info for dropped tables
				objects = append(objects, wal.DDLObject{
					Type:     "table",
					Identity: fmt.Sprintf("%s.%s", schema, table),
					Schema:   schema,
				})
				continue
			}
			objects = append(objects, *obj)
		}
	}

	// Create a DDL event
	ddlEvent := wal.DDLEvent{
		DDL:        statement,
		SchemaName: schemaName,
		CommandTag: commandTag,
		Objects:    objects,
	}

	// Marshal the DDL event to JSON
	contentBytes, err := jsonlib.Marshal(ddlEvent)
	if err != nil {
		return fmt.Errorf("marshaling DDL event: %w", err)
	}

	// Create WAL data for the logical message
	walData := &wal.Data{
		Action:    wal.LogicalMessageAction,
		Timestamp: timestamp,
		LSN:       wal.ZeroLSN,
		Prefix:    wal.DDLPrefix,
		Content:   string(contentBytes),
	}

	// Create the WAL event
	walEvent := &wal.Event{
		Data: walData,
	}

	// Process the event
	if err := r.processor.ProcessWALEvent(ctx, walEvent); err != nil {
		return fmt.Errorf("processing WAL event: %w", err)
	}

	return nil
}

const tableObjectQuery = `
	SELECT jsonb_build_object(
		'type', 'table',
		'identity', n.nspname || '.' || c.relname,
		'schema', n.nspname,
		'oid', c.oid::text,
		'pgstream_id', COALESCE(t.id::text, pgstream.create_table_mapping(c.oid)::text)
	) || pgstream.get_table_metadata(c.oid)
	FROM pg_class c
	JOIN pg_namespace n ON c.relnamespace = n.oid
	LEFT JOIN pgstream.table_ids t ON c.oid = t.oid
	WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')
`

func (r *PGSnapshotWALRestore) getTableObject(ctx context.Context, schema, table string) (*wal.DDLObject, error) {
	var tableObjJSON []byte
	dest := []any{&tableObjJSON}
	err := r.querier.QueryRow(ctx, dest, tableObjectQuery, schema, table)
	if err != nil {
		return nil, err
	}

	var tableObj wal.DDLObject
	if err := jsonlib.Unmarshal(tableObjJSON, &tableObj); err != nil {
		return nil, fmt.Errorf("unmarshaling table object: %w", err)
	}

	return &tableObj, nil
}

// isDDLStatement checks if a statement is a DDL statement
func isDDLStatement(statement string) bool {
	upperStatement := strings.ToUpper(statement)
	for _, prefix := range ddlCommandTags {
		if strings.HasPrefix(upperStatement, prefix) {
			return true
		}
	}

	return false
}

// extractCommandTag extracts the command tag (e.g., "CREATE TABLE") from a DDL statement
func extractCommandTag(statement string) string {
	upperStatement := strings.ToUpper(strings.TrimSpace(statement))

	for _, pattern := range ddlCommandTags {
		if strings.HasPrefix(upperStatement, pattern) {
			return pattern
		}
	}

	// If no match found, return the first two words as a fallback
	words := strings.Fields(upperStatement)
	if len(words) >= 2 {
		return words[0] + " " + words[1]
	} else if len(words) == 1 {
		return words[0]
	}

	return "UNKNOWN"
}

// Regular expressions for extracting schema and table names from DDL statements
var (
	createTableRegex = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"?(\w+)"?\."?(\w+)"?|"?(\w+)"?)`)
	alterTableRegex  = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:"?(\w+)"?\."?(\w+)"?|"?(\w+)"?)`)
	dropTableRegex   = regexp.MustCompile(`(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:"?(\w+)"?\."?(\w+)"?|"?(\w+)"?)`)
)

// extractSchemaAndObjects extracts the schema name and object names from a DDL statement
func extractSchemaAndObjects(statement, commandTag string) (string, []string) {
	var schema string
	var objects []string

	// Determine which regex to use based on command tag
	var re *regexp.Regexp
	switch {
	case strings.HasPrefix(commandTag, "CREATE TABLE"):
		re = createTableRegex
	case strings.HasPrefix(commandTag, "ALTER TABLE"):
		re = alterTableRegex
	case strings.HasPrefix(commandTag, "DROP TABLE"):
		re = dropTableRegex
	default:
		return publicSchema, objects
	}

	matches := re.FindStringSubmatch(statement)
	if len(matches) > 0 {
		// Check if schema.table format was matched
		if matches[1] != "" && matches[2] != "" {
			schema = matches[1]
			objects = append(objects, matches[1]+"."+matches[2])
		} else if matches[3] != "" {
			// Only table name was provided, assume public schema
			schema = publicSchema
			objects = append(objects, "public."+matches[3])
		}
	}

	if schema == "" {
		schema = publicSchema
	}

	return schema, objects
}

// parseSchemaTable parses schema.table format, using defaultSchema if not qualified
func parseSchemaTable(qualifiedName, defaultSchema string) (string, string) {
	parts := strings.Split(qualifiedName, ".")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return defaultSchema, qualifiedName
}
