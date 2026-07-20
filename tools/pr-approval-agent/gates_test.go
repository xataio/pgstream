// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
)

func files(specs ...changedFile) []changedFile { return specs }

func f(path string, add, del int) changedFile {
	return changedFile{Path: path, Additions: add, Deletions: del}
}

func TestMigrationIsDenied(t *testing.T) {
	c := classify(files(f("migrations/postgres/0001_init.sql", 10, 0)))
	if c.Tier != "T2-never" {
		t.Fatalf("tier = %q, want T2-never", c.Tier)
	}
	if !contains(c.DenyCategories, "migration") {
		t.Fatalf("deny categories = %v, want migration", c.DenyCategories)
	}
	if g := runGates(c, "MERGEABLE"); g.Verdict != "ESCALATE" {
		t.Fatalf("verdict = %q, want ESCALATE", g.Verdict)
	}
}

func TestDocsOnlyIsTrivial(t *testing.T) {
	c := classify(files(f("README.md", 5, 2), f("docs/guide.md", 1, 0)))
	if c.Tier != "T0-trivial" || !c.TrivialOnly {
		t.Fatalf("got tier=%q trivialOnly=%v", c.Tier, c.TrivialOnly)
	}
	if g := runGates(c, "MERGEABLE"); g.Verdict != "PASS" {
		t.Fatalf("verdict = %q, want PASS", g.Verdict)
	}
}

func TestNormalGoChangeIsAgentReviewed(t *testing.T) {
	c := classify(files(f("pkg/foo/bar.go", 10, 1)))
	if c.Tier != "T1-agent" {
		t.Fatalf("tier = %q, want T1-agent", c.Tier)
	}
	if g := runGates(c, "MERGEABLE"); g.Verdict != "PASS" {
		t.Fatalf("verdict = %q, want PASS", g.Verdict)
	}
}

func TestScrutinyChangesAreFlaggedNotDenied(t *testing.T) {
	cases := map[string]string{
		"pkg/wal/replication/handler.go":  "replication-core",
		"pkg/wal/checkpointer/cp.go":      "checkpoint-core",
		"pkg/wal/listener/listener.go":    "wal-listener",
		"pkg/snapshot/snapshot.go":        "snapshot-core",
		"pkg/wal/processor/postgres/w.go": "postgres-writer",
	}
	for path, flag := range cases {
		c := classify(files(f(path, 40, 3)))
		if c.Tier != "T1-agent" {
			t.Errorf("%s: tier = %q, want T1-agent", path, c.Tier)
		}
		if !contains(c.ScrutinyFlags, flag) {
			t.Errorf("%s: scrutiny flags = %v, want %s", path, c.ScrutinyFlags, flag)
		}
		if g := runGates(c, "MERGEABLE"); g.Verdict != "PASS" {
			t.Errorf("%s: verdict = %q, want PASS", path, g.Verdict)
		}
	}
}

func TestTestsDoNotCountTowardSizeCeiling(t *testing.T) {
	c := classify(files(f("pkg/a_test.go", 900, 0)))
	if c.SubstantiveLines != 0 {
		t.Fatalf("substantive lines = %d, want 0", c.SubstantiveLines)
	}
	if c.Tier != "T0-trivial" {
		t.Fatalf("tier = %q, want T0-trivial", c.Tier)
	}
}

func TestLargeSubstantiveChangeEscalatesOnSize(t *testing.T) {
	c := classify(files(f("pkg/a.go", 500, 400)))
	g := runGates(c, "MERGEABLE")
	if g.Verdict != "ESCALATE" {
		t.Fatalf("verdict = %q, want ESCALATE", g.Verdict)
	}
	if !anyContains(g.Reasons, "ceiling") {
		t.Fatalf("reasons = %v, want a ceiling mention", g.Reasons)
	}
}

func TestDotfileDenyPathsMatch(t *testing.T) {
	cases := map[string]string{
		".github/workflows/build.yml":      "ci-cd",
		".goreleaser.yaml":                 "release/supply-chain",
		"Dockerfile":                       "release/supply-chain",
		"go.mod":                           "dependencies",
		"go.sum":                           "dependencies",
		"tools/pr-approval-agent/gates.go": "review-agent-self",
		"LICENSE":                          "legal",
	}
	for path, cat := range cases {
		c := classify(files(f(path, 1, 0)))
		if !contains(c.DenyCategories, cat) {
			t.Errorf("%s: deny categories = %v, want %s", path, c.DenyCategories, cat)
		}
		if g := runGates(c, "MERGEABLE"); g.Verdict != "ESCALATE" {
			t.Errorf("%s: verdict = %q, want ESCALATE", path, g.Verdict)
		}
	}
}

func TestDenyWinsMixedWithTrivial(t *testing.T) {
	c := classify(files(f("README.md", 1, 0), f("go.mod", 1, 0)))
	if c.Tier != "T2-never" || c.TrivialOnly {
		t.Fatalf("got tier=%q trivialOnly=%v", c.Tier, c.TrivialOnly)
	}
}

func TestConflictEscalates(t *testing.T) {
	c := classify(files(f("pkg/a.go", 1, 0)))
	if g := runGates(c, "CONFLICTING"); g.Verdict != "ESCALATE" {
		t.Fatalf("conflicting verdict = %q, want ESCALATE", g.Verdict)
	}
}

func TestGlobToRegexp(t *testing.T) {
	cases := []struct {
		pattern, name string
		want          bool
	}{
		{"**/*_test.go", "pkg/wal/foo_test.go", true},
		{"**/*_test.go", "foo_test.go", true},
		{"pkg/wal/**/*.go", "pkg/wal/replication/h.go", true},
		{"pkg/wal/**/*.go", "pkg/other/h.go", false},
		{"*.md", "README.md", true},
		{"*.md", "docs/x.md", false},
	}
	for _, tc := range cases {
		if got := matchGlob(tc.pattern, tc.name); got != tc.want {
			t.Errorf("matchGlob(%q, %q) = %v, want %v", tc.pattern, tc.name, got, tc.want)
		}
	}
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

func anyContains(list []string, sub string) bool {
	for _, s := range list {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
