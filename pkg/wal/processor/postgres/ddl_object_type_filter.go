// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"strings"

	"github.com/xataio/pgstream/pkg/wal"
)

// ddlObjectTypeCategories maps user-facing category names to DDL object type
// values as reported by pg_event_trigger_ddl_commands() and
// pg_event_trigger_dropped_objects().
var ddlObjectTypeCategories = map[string][]string{
	"tables":             {"table", "table column"},
	"sequences":          {"sequence"},
	"types":              {"type", "domain"},
	"indexes":            {"index"},
	"constraints":        {"table constraint"},
	"functions":          {"function", "aggregate", "procedure"},
	"views":              {"view"},
	"materialized_views": {"materialized view"},
	"triggers":           {"trigger"},
	"event_triggers":     {"event trigger"},
	"policies":           {"policy"},
	"rules":              {"rule"},
	"extensions":         {"extension"},
	"collations":         {"collation"},
	"text_search":        {"text search configuration", "text search dictionary", "text search parser", "text search template"},
}

// commandTagCategories maps command tag prefixes to categories for fallback
// when DDL events have no objects.
var commandTagCategories = map[string]string{
	"CREATE TABLE":                     "tables",
	"ALTER TABLE":                      "tables",
	"DROP TABLE":                       "tables",
	"CREATE SEQUENCE":                  "sequences",
	"ALTER SEQUENCE":                   "sequences",
	"DROP SEQUENCE":                    "sequences",
	"CREATE TYPE":                      "types",
	"ALTER TYPE":                       "types",
	"DROP TYPE":                        "types",
	"CREATE DOMAIN":                    "types",
	"ALTER DOMAIN":                     "types",
	"DROP DOMAIN":                      "types",
	"CREATE INDEX":                     "indexes",
	"ALTER INDEX":                      "indexes",
	"DROP INDEX":                       "indexes",
	"CREATE FUNCTION":                  "functions",
	"ALTER FUNCTION":                   "functions",
	"DROP FUNCTION":                    "functions",
	"CREATE AGGREGATE":                 "functions",
	"DROP AGGREGATE":                   "functions",
	"CREATE PROCEDURE":                 "functions",
	"ALTER PROCEDURE":                  "functions",
	"DROP PROCEDURE":                   "functions",
	"CREATE VIEW":                      "views",
	"ALTER VIEW":                       "views",
	"DROP VIEW":                        "views",
	"CREATE MATERIALIZED VIEW":         "materialized_views",
	"ALTER MATERIALIZED VIEW":          "materialized_views",
	"DROP MATERIALIZED VIEW":           "materialized_views",
	"CREATE TRIGGER":                   "triggers",
	"ALTER TRIGGER":                    "triggers",
	"DROP TRIGGER":                     "triggers",
	"CREATE EVENT TRIGGER":             "event_triggers",
	"ALTER EVENT TRIGGER":              "event_triggers",
	"DROP EVENT TRIGGER":               "event_triggers",
	"CREATE POLICY":                    "policies",
	"ALTER POLICY":                     "policies",
	"DROP POLICY":                      "policies",
	"CREATE RULE":                      "rules",
	"ALTER RULE":                       "rules",
	"DROP RULE":                        "rules",
	"CREATE EXTENSION":                 "extensions",
	"ALTER EXTENSION":                  "extensions",
	"DROP EXTENSION":                   "extensions",
	"CREATE COLLATION":                 "collations",
	"ALTER COLLATION":                  "collations",
	"DROP COLLATION":                   "collations",
	"CREATE TEXT SEARCH CONFIGURATION": "text_search",
	"ALTER TEXT SEARCH CONFIGURATION":  "text_search",
	"DROP TEXT SEARCH CONFIGURATION":   "text_search",
}

// ddlObjectTypeFilter determines which DDL events should be skipped based on
// user-specified include or exclude category lists.
type ddlObjectTypeFilter struct {
	excludedDDLTypes   map[string]struct{}
	excludedCategories map[string]struct{}
}

// newDDLObjectTypeFilter creates a ddlObjectTypeFilter from include/exclude
// category lists. Only one of include or exclude can be set (not both).
// Returns nil if neither is set (no filtering).
func newDDLObjectTypeFilter(include, exclude []string) (*ddlObjectTypeFilter, error) {
	if len(include) > 0 && len(exclude) > 0 {
		return nil, fmt.Errorf("include_ddl_object_types and exclude_ddl_object_types cannot both be set")
	}

	if len(include) == 0 && len(exclude) == 0 {
		return nil, nil
	}

	f := &ddlObjectTypeFilter{
		excludedDDLTypes:   make(map[string]struct{}),
		excludedCategories: make(map[string]struct{}),
	}

	if len(include) > 0 {
		includedSet := make(map[string]struct{}, len(include))
		for _, cat := range include {
			if _, ok := ddlObjectTypeCategories[cat]; !ok {
				return nil, fmt.Errorf("unknown DDL object type category: %q", cat)
			}
			includedSet[cat] = struct{}{}
		}
		for cat, types := range ddlObjectTypeCategories {
			if _, included := includedSet[cat]; !included {
				f.excludedCategories[cat] = struct{}{}
				for _, t := range types {
					f.excludedDDLTypes[t] = struct{}{}
				}
			}
		}
	} else {
		for _, cat := range exclude {
			types, ok := ddlObjectTypeCategories[cat]
			if !ok {
				return nil, fmt.Errorf("unknown DDL object type category: %q", cat)
			}
			f.excludedCategories[cat] = struct{}{}
			for _, t := range types {
				f.excludedDDLTypes[t] = struct{}{}
			}
		}
	}

	return f, nil
}

// shouldSkipDDL returns true if the DDL event should be skipped based on the
// object types involved.
func (f *ddlObjectTypeFilter) shouldSkipDDL(ddlEvent *wal.DDLEvent) bool {
	if f == nil || ddlEvent == nil {
		return false
	}

	// If there are objects, check their types. A single DDL statement (e.g.,
	// CREATE TABLE with PRIMARY KEY) can produce multiple objects of different
	// types (table + index). Only skip if ALL objects are of excluded types;
	// if any object is of an included type, the event should be executed.
	if len(ddlEvent.Objects) > 0 {
		for _, obj := range ddlEvent.Objects {
			objType := strings.ToLower(obj.Type)
			if _, excluded := f.excludedDDLTypes[objType]; !excluded {
				return false
			}
		}
		return true
	}

	// Fallback: parse the command tag
	tag := ddlEvent.CommandTag
	for prefix, cat := range commandTagCategories {
		if strings.HasPrefix(tag, prefix) {
			_, excluded := f.excludedCategories[cat]
			return excluded
		}
	}

	return false
}
