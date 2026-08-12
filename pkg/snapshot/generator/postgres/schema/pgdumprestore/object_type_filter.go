// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"bufio"
	"bytes"
	"regexp"
	"strings"

	"github.com/xataio/pgstream/internal/objecttype"
)

// objectTypeCategory maps user-facing category names to pg_dump TOC Type values.
var objectTypeCategories = map[string][]string{
	"tables":             {"TABLE", "DEFAULT"},
	"sequences":          {"SEQUENCE", "SEQUENCE OWNED BY"},
	"types":              {"TYPE", "DOMAIN"},
	"indexes":            {"INDEX"},
	"constraints":        {"CONSTRAINT", "FK CONSTRAINT"},
	"functions":          {"FUNCTION", "AGGREGATE", "PROCEDURE"},
	"views":              {"VIEW"},
	"materialized_views": {"MATERIALIZED VIEW"},
	"triggers":           {"TRIGGER"},
	"event_triggers":     {"EVENT TRIGGER"},
	"policies":           {"POLICY", "ROW SECURITY"},
	"rules":              {"RULE"},
	"comments":           {"COMMENT"},
	"extensions":         {"EXTENSION"},
	"collations":         {"COLLATION"},
	"text_search":        {"TEXT SEARCH CONFIGURATION", "TEXT SEARCH DICTIONARY", "TEXT SEARCH PARSER", "TEXT SEARCH TEMPLATE"},
}

// tocHeaderRegex matches pg_dump TOC comment headers like:
// -- Name: my_func; Type: FUNCTION; Schema: public; Owner: postgres
var tocHeaderRegex = regexp.MustCompile(`^--\s*Name:.*;\s*Type:\s*([^;]+)\s*;`)

// parseTOCHeader extracts the object Type value from a pg_dump TOC comment header line.
// Returns the type string and true if the line is a TOC header, or ("", false) otherwise.
func parseTOCHeader(line string) (string, bool) {
	matches := tocHeaderRegex.FindStringSubmatch(line)
	if len(matches) < 2 {
		return "", false
	}
	return strings.TrimSpace(matches[1]), true
}

// objectTypeFilter determines which pg_dump object types should be excluded
// based on user-specified include or exclude category lists.
type objectTypeFilter struct {
	*objecttype.Filter
}

// newObjectTypeFilter creates an objectTypeFilter from include/exclude category lists.
// Only one of include or exclude can be set (not both).
// Returns nil if neither is set (no filtering).
func newObjectTypeFilter(include, exclude []string) (*objectTypeFilter, error) {
	f, err := objecttype.NewFilter(objectTypeCategories, include, exclude)
	if err != nil || f == nil {
		return nil, err
	}
	return &objectTypeFilter{Filter: f}, nil
}

// isExcluded returns true if the given pg_dump Type value should be excluded.
// SCHEMA type is never excluded (required for namespace resolution).
func (f *objectTypeFilter) isExcluded(pgdumpType string) bool {
	if f == nil || pgdumpType == "SCHEMA" {
		return false
	}
	return f.IsTypeExcluded(pgdumpType)
}

// isCategoryExcluded returns true if the given user-facing category is excluded.
func (f *objectTypeFilter) isCategoryExcluded(category string) bool {
	if f == nil {
		return false
	}
	return f.IsCategoryExcluded(category)
}

// cleanupStatementPrefixes maps SQL cleanup statement prefixes (from pg_dump
// --clean --if-exists output) to the object type category they belong to. The
// prefixes are matched with a trailing space, so that DROP TABLE does not also
// match DROP TABLESPACE. No prefix is a prefix of another, so the map iteration
// order does not affect the result.
var cleanupStatementPrefixes = map[string]string{
	"DROP POLICY":            "policies",
	"DROP TRIGGER":           "triggers",
	"DROP RULE":              "rules",
	"DROP INDEX":             "indexes",
	"DROP FUNCTION":          "functions",
	"DROP AGGREGATE":         "functions",
	"DROP PROCEDURE":         "functions",
	"DROP VIEW":              "views",
	"DROP MATERIALIZED VIEW": "materialized_views",
	"DROP TEXT SEARCH":       "text_search",
	"DROP COLLATION":         "collations",
	"DROP EXTENSION":         "extensions",
	"DROP EVENT TRIGGER":     "event_triggers",
	"DROP SEQUENCE":          "sequences",
	"DROP TABLE":             "tables",
	"DROP TYPE":              "types",
	"DROP DOMAIN":            "types",
	"DROP SCHEMA":            "schemas",
	"COMMENT ON":             "comments",
}

// filterCleanupDump removes lines from a cleanup dump that belong to excluded
// object type categories. This prevents errors like "relation does not exist"
// when DROP POLICY/TRIGGER/RULE statements reference tables that don't yet
// exist on the target.
func (f *objectTypeFilter) filterCleanupDump(cleanupDump []byte) []byte {
	if f == nil {
		return cleanupDump
	}

	scanner := bufio.NewScanner(bytes.NewReader(cleanupDump))
	var filtered strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		if f.shouldSkipCleanupLine(line) {
			continue
		}
		filtered.WriteString(line)
		filtered.WriteString("\n")
	}
	return []byte(filtered.String())
}

// shouldSkipCleanupLine returns true if a cleanup dump line should be skipped
// because it belongs to an excluded object type category.
func (f *objectTypeFilter) shouldSkipCleanupLine(line string) bool {
	if f == nil {
		return false
	}
	for prefix, cat := range cleanupStatementPrefixes {
		if strings.HasPrefix(line, prefix+" ") {
			// SCHEMA is never excluded
			if cat == "schemas" {
				return false
			}
			return f.isCategoryExcluded(cat)
		}
	}
	return false
}
