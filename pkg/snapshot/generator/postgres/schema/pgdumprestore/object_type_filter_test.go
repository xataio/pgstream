// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseTOCHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		line     string
		wantType string
		wantOK   bool
	}{
		{
			name:     "function header",
			line:     "-- Name: my_func; Type: FUNCTION; Schema: public; Owner: postgres",
			wantType: "FUNCTION",
			wantOK:   true,
		},
		{
			name:     "table header",
			line:     "-- Name: users; Type: TABLE; Schema: public; Owner: postgres",
			wantType: "TABLE",
			wantOK:   true,
		},
		{
			name:     "index header",
			line:     "-- Name: area_alias_idx_txt; Type: INDEX; Schema: musicbrainz; Owner: postgres",
			wantType: "INDEX",
			wantOK:   true,
		},
		{
			name:     "schema header",
			line:     "-- Name: musicbrainz; Type: SCHEMA; Schema: -; Owner: postgres",
			wantType: "SCHEMA",
			wantOK:   true,
		},
		{
			name:     "fk constraint header",
			line:     "-- Name: alternative_medium alternative_medium_fk_medium; Type: FK CONSTRAINT; Schema: musicbrainz; Owner: postgres",
			wantType: "FK CONSTRAINT",
			wantOK:   true,
		},
		{
			name:     "constraint header",
			line:     "-- Name: alternative_medium alternative_medium_pkey; Type: CONSTRAINT; Schema: musicbrainz; Owner: postgres",
			wantType: "CONSTRAINT",
			wantOK:   true,
		},
		{
			name:     "trigger header",
			line:     "-- Name: alternative_medium_track a_del_alternative_medium_track; Type: TRIGGER; Schema: musicbrainz; Owner: postgres",
			wantType: "TRIGGER",
			wantOK:   true,
		},
		{
			name:     "aggregate header",
			line:     "-- Name: median(integer); Type: AGGREGATE; Schema: musicbrainz; Owner: postgres",
			wantType: "AGGREGATE",
			wantOK:   true,
		},
		{
			name:     "text search configuration header",
			line:     "-- Name: mb_simple; Type: TEXT SEARCH CONFIGURATION; Schema: musicbrainz; Owner: postgres",
			wantType: "TEXT SEARCH CONFIGURATION",
			wantOK:   true,
		},
		{
			name:     "sequence header",
			line:     "-- Name: alternative_medium_id_seq; Type: SEQUENCE; Schema: musicbrainz; Owner: postgres",
			wantType: "SEQUENCE",
			wantOK:   true,
		},
		{
			name:     "default header",
			line:     "-- Name: alternative_medium id; Type: DEFAULT; Schema: musicbrainz; Owner: postgres",
			wantType: "DEFAULT",
			wantOK:   true,
		},
		{
			name:     "extension header",
			line:     "-- Name: cube; Type: EXTENSION; Schema: -; Owner: -",
			wantType: "EXTENSION",
			wantOK:   true,
		},
		{
			name:     "comment header",
			line:     "-- Name: EXTENSION cube; Type: COMMENT; Schema: -; Owner:",
			wantType: "COMMENT",
			wantOK:   true,
		},
		{
			name:     "collation header",
			line:     "-- Name: musicbrainz; Type: COLLATION; Schema: musicbrainz; Owner: postgres",
			wantType: "COLLATION",
			wantOK:   true,
		},
		{
			name:     "type header",
			line:     "-- Name: cover_art_presence; Type: TYPE; Schema: musicbrainz; Owner: postgres",
			wantType: "TYPE",
			wantOK:   true,
		},
		{
			name:   "regular comment line",
			line:   "-- PostgreSQL database dump",
			wantOK: false,
		},
		{
			name:   "empty line",
			line:   "",
			wantOK: false,
		},
		{
			name:   "SQL statement",
			line:   "CREATE TABLE users (id integer);",
			wantOK: false,
		},
		{
			name:   "separator comment",
			line:   "--",
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotType, gotOK := parseTOCHeader(tc.line)
			require.Equal(t, tc.wantOK, gotOK)
			if gotOK {
				require.Equal(t, tc.wantType, gotType)
			}
		})
	}
}

func TestNewObjectTypeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		include []string
		exclude []string

		wantNil bool
		wantErr bool
	}{
		{
			name:    "no filter configured",
			wantNil: true,
		},
		{
			name:    "include only",
			include: []string{"tables", "sequences", "types"},
		},
		{
			name:    "exclude only",
			exclude: []string{"functions", "views"},
		},
		{
			name:    "both set - error",
			include: []string{"tables"},
			exclude: []string{"functions"},
			wantErr: true,
		},
		{
			name:    "unknown include category",
			include: []string{"tables", "unknown_category"},
			wantErr: true,
		},
		{
			name:    "unknown exclude category",
			exclude: []string{"unknown_category"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f, err := newObjectTypeFilter(tc.include, tc.exclude)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tc.wantNil {
				require.Nil(t, f)
				return
			}

			require.NotNil(t, f)
		})
	}
}

func TestObjectTypeFilter_IsExcluded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		include    []string
		exclude    []string
		pgdumpType string

		wantExcluded bool
	}{
		{
			name:         "nil filter - never excluded",
			pgdumpType:   "FUNCTION",
			wantExcluded: false,
		},
		{
			name:         "include tables only - function excluded",
			include:      []string{"tables", "sequences", "types"},
			pgdumpType:   "FUNCTION",
			wantExcluded: true,
		},
		{
			name:         "include tables only - table included",
			include:      []string{"tables", "sequences", "types"},
			pgdumpType:   "TABLE",
			wantExcluded: false,
		},
		{
			name:         "include tables only - DEFAULT included (part of tables)",
			include:      []string{"tables", "sequences", "types"},
			pgdumpType:   "DEFAULT",
			wantExcluded: false,
		},
		{
			name:         "include tables only - SEQUENCE included",
			include:      []string{"tables", "sequences", "types"},
			pgdumpType:   "SEQUENCE",
			wantExcluded: false,
		},
		{
			name:         "include tables only - INDEX excluded",
			include:      []string{"tables", "sequences", "types"},
			pgdumpType:   "INDEX",
			wantExcluded: true,
		},
		{
			name:         "exclude functions - function excluded",
			exclude:      []string{"functions"},
			pgdumpType:   "FUNCTION",
			wantExcluded: true,
		},
		{
			name:         "exclude functions - aggregate excluded",
			exclude:      []string{"functions"},
			pgdumpType:   "AGGREGATE",
			wantExcluded: true,
		},
		{
			name:         "exclude functions - table not excluded",
			exclude:      []string{"functions"},
			pgdumpType:   "TABLE",
			wantExcluded: false,
		},
		{
			name:         "SCHEMA is never excluded",
			include:      []string{"tables"},
			pgdumpType:   "SCHEMA",
			wantExcluded: false,
		},
		{
			name:         "exclude views - VIEW excluded",
			exclude:      []string{"views"},
			pgdumpType:   "VIEW",
			wantExcluded: true,
		},
		{
			name:         "exclude text_search - TEXT SEARCH CONFIGURATION excluded",
			exclude:      []string{"text_search"},
			pgdumpType:   "TEXT SEARCH CONFIGURATION",
			wantExcluded: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var f *objectTypeFilter
			if tc.include != nil || tc.exclude != nil {
				var err error
				f, err = newObjectTypeFilter(tc.include, tc.exclude)
				require.NoError(t, err)
			}

			got := f.isExcluded(tc.pgdumpType)
			require.Equal(t, tc.wantExcluded, got)
		})
	}
}

func TestObjectTypeFilter_IsCategoryExcluded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		include  []string
		exclude  []string
		category string

		wantExcluded bool
	}{
		{
			name:         "nil filter",
			category:     "functions",
			wantExcluded: false,
		},
		{
			name:         "include mode - included category",
			include:      []string{"tables", "sequences"},
			category:     "tables",
			wantExcluded: false,
		},
		{
			name:         "include mode - excluded category",
			include:      []string{"tables", "sequences"},
			category:     "functions",
			wantExcluded: true,
		},
		{
			name:         "exclude mode - excluded category",
			exclude:      []string{"functions", "views"},
			category:     "functions",
			wantExcluded: true,
		},
		{
			name:         "exclude mode - not excluded category",
			exclude:      []string{"functions", "views"},
			category:     "tables",
			wantExcluded: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var f *objectTypeFilter
			if tc.include != nil || tc.exclude != nil {
				var err error
				f, err = newObjectTypeFilter(tc.include, tc.exclude)
				require.NoError(t, err)
			}

			got := f.isCategoryExcluded(tc.category)
			require.Equal(t, tc.wantExcluded, got)
		})
	}
}

func TestObjectTypeFilter_FilterCleanupDump(t *testing.T) {
	t.Parallel()

	cleanupDump := `DROP POLICY IF EXISTS posts_select_published ON app.posts;
DROP POLICY IF EXISTS posts_modify_own ON app.posts;
ALTER TABLE IF EXISTS ONLY app.posts DROP CONSTRAINT IF EXISTS posts_category_id_fkey;
ALTER TABLE IF EXISTS ONLY app.posts DROP CONSTRAINT IF EXISTS posts_author_id_fkey;
DROP TRIGGER IF EXISTS trg_users_updated_at ON app.users;
DROP TRIGGER IF EXISTS trg_posts_updated_at ON app.posts;
DROP RULE IF EXISTS protect_audit_update ON app.audit_log;
DROP INDEX IF EXISTS app.idx_users_email;
DROP INDEX IF EXISTS app.idx_posts_slug;
ALTER TABLE IF EXISTS ONLY app.users DROP CONSTRAINT IF EXISTS users_pkey;
ALTER TABLE IF EXISTS app.posts ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS app.users_id_seq;
DROP VIEW IF EXISTS app.user_stats;
DROP TABLE IF EXISTS app.users;
DROP TABLE IF EXISTS app.posts;
DROP FUNCTION IF EXISTS app.my_func();
DROP AGGREGATE IF EXISTS app.median(integer);
DROP TYPE IF EXISTS app.status;
DROP DOMAIN IF EXISTS app.email;
DROP COLLATION IF EXISTS app.case_insensitive;
DROP MATERIALIZED VIEW IF EXISTS analytics.top_posts;
DROP TEXT SEARCH CONFIGURATION IF EXISTS app.english_unaccent;
DROP SCHEMA IF EXISTS app;
`

	tests := []struct {
		name            string
		include         []string
		wantContains    []string
		wantNotContains []string
	}{
		{
			name:    "include tables sequences types only",
			include: []string{"tables", "sequences", "types"},
			wantContains: []string{
				"DROP TABLE IF EXISTS app.users",
				"DROP TABLE IF EXISTS app.posts",
				"DROP SEQUENCE IF EXISTS app.users_id_seq",
				"DROP TYPE IF EXISTS app.status",
				"DROP DOMAIN IF EXISTS app.email",
				"DROP SCHEMA IF EXISTS app",
				// ALTER TABLE lines are not categorized as any specific type,
				// so they pass through
				"ALTER TABLE IF EXISTS ONLY app.posts DROP CONSTRAINT",
				"ALTER TABLE IF EXISTS app.posts ALTER COLUMN id DROP DEFAULT",
			},
			wantNotContains: []string{
				"DROP POLICY",
				"DROP TRIGGER",
				"DROP RULE",
				"DROP INDEX",
				"DROP VIEW",
				"DROP FUNCTION",
				"DROP AGGREGATE",
				"DROP COLLATION",
				"DROP MATERIALIZED VIEW",
				"DROP TEXT SEARCH",
			},
		},
		{
			name:    "include everything",
			include: []string{"tables", "sequences", "types", "indexes", "constraints", "functions", "views", "materialized_views", "triggers", "event_triggers", "policies", "rules", "comments", "extensions", "collations", "text_search"},
			wantContains: []string{
				"DROP TABLE",
				"DROP POLICY",
				"DROP TRIGGER",
				"DROP RULE",
				"DROP INDEX",
				"DROP VIEW",
				"DROP FUNCTION",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f, err := newObjectTypeFilter(tc.include, nil)
			require.NoError(t, err)

			result := string(f.filterCleanupDump([]byte(cleanupDump)))

			for _, s := range tc.wantContains {
				require.Contains(t, result, s, "expected cleanup to contain: %s", s)
			}
			for _, s := range tc.wantNotContains {
				require.NotContains(t, result, s, "expected cleanup NOT to contain: %s", s)
			}
		})
	}
}

func TestObjectTypeFilter_FilterCleanupDump_NilFilter(t *testing.T) {
	t.Parallel()

	input := []byte("DROP FUNCTION IF EXISTS app.my_func();\nDROP TABLE IF EXISTS app.users;\n")
	var f *objectTypeFilter
	result := f.filterCleanupDump(input)
	require.Equal(t, input, result)
}

func TestParseDump_WithObjectTypeFilter(t *testing.T) {
	t.Parallel()

	// Test that parseDump excludes functions and aggregates when filtered
	dumpInput := `--
-- PostgreSQL database dump
--

SET statement_timeout = 0;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA public;

--
-- Name: my_func(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.my_func() RETURNS void
    LANGUAGE sql
    AS $$ SELECT 1; $$;

ALTER FUNCTION public.my_func() OWNER TO postgres;

--
-- Name: my_agg(integer); Type: AGGREGATE; Schema: public; Owner: postgres
--

CREATE AGGREGATE public.my_agg(integer) (
    SFUNC = array_append,
    STYPE = integer[]
);

--
-- Name: users; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.users_id_seq
    START WITH 1
    INCREMENT BY 1;

--
-- Name: users id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);

--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);

--
-- Name: users_name_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX users_name_idx ON public.users USING btree (name);
`

	t.Run("exclude functions", func(t *testing.T) {
		t.Parallel()

		filter, err := newObjectTypeFilter(nil, []string{"functions"})
		require.NoError(t, err)

		sg := &SnapshotGenerator{
			objectTypeFilter: filter,
		}

		d := sg.parseDump([]byte(dumpInput))

		// Functions and aggregates should be excluded from the filtered dump
		filteredStr := string(d.filtered)
		require.NotContains(t, filteredStr, "CREATE FUNCTION")
		require.NotContains(t, filteredStr, "CREATE AGGREGATE")
		require.NotContains(t, filteredStr, "ALTER FUNCTION")

		// Tables and sequences should still be present
		require.Contains(t, filteredStr, "CREATE TABLE public.users")
		require.Contains(t, filteredStr, "CREATE SCHEMA public")
	})

	t.Run("include only tables and sequences", func(t *testing.T) {
		t.Parallel()

		filter, err := newObjectTypeFilter([]string{"tables", "sequences"}, nil)
		require.NoError(t, err)

		sg := &SnapshotGenerator{
			objectTypeFilter: filter,
		}

		d := sg.parseDump([]byte(dumpInput))

		filteredStr := string(d.filtered)

		// Functions, aggregates, indexes, constraints should be excluded
		require.NotContains(t, filteredStr, "CREATE FUNCTION")
		require.NotContains(t, filteredStr, "CREATE AGGREGATE")

		// Tables and sequences should be present
		require.Contains(t, filteredStr, "CREATE TABLE public.users")
		require.Contains(t, filteredStr, "CREATE SEQUENCE public.users_id_seq")

		// SCHEMA is always included
		require.Contains(t, filteredStr, "CREATE SCHEMA public")

		// Preamble (SET statements) should be included
		require.Contains(t, filteredStr, "SET statement_timeout = 0")
	})

	t.Run("no filter - everything included", func(t *testing.T) {
		t.Parallel()

		sg := &SnapshotGenerator{}

		d := sg.parseDump([]byte(dumpInput))

		filteredStr := string(d.filtered)

		// Everything should be present
		require.Contains(t, filteredStr, "CREATE FUNCTION")
		require.Contains(t, filteredStr, "CREATE AGGREGATE")
		require.Contains(t, filteredStr, "CREATE TABLE public.users")
		require.Contains(t, filteredStr, "CREATE SEQUENCE public.users_id_seq")
	})
}
