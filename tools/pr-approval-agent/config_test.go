// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadPolicyFromYAML(t *testing.T) {
	yaml := `
maxSubstantiveLines: 50
denyPaths:
  - pattern: '^infra/'
    category: infrastructure
scrutinyPaths:
  - pattern: '^internal/auth/'
    category: auth-core
inertPaths:
  - '\.md$'
trivialPaths:
  - '\.md$'
  - '_test\.go$'
`
	path := filepath.Join(t.TempDir(), "policy.yml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := loadPolicy(path)
	if err != nil {
		t.Fatalf("loadPolicy: %v", err)
	}

	// Custom deny path applies; pgstream's built-in ones do NOT (full replace).
	c := p.classify(files(f("infra/terraform.tf", 3, 0)))
	if !contains(c.DenyCategories, "infrastructure") {
		t.Fatalf("deny categories = %v, want infrastructure", c.DenyCategories)
	}
	if c2 := p.classify(files(f("migrations/x.sql", 1, 0))); len(c2.DenyCategories) != 0 {
		t.Fatalf("built-in deny paths should not apply under a custom policy, got %v", c2.DenyCategories)
	}

	// Custom threshold applies; omitted MaxFiles falls back to the default.
	if p.MaxLines != 50 {
		t.Fatalf("MaxLines = %d, want 50", p.MaxLines)
	}
	if p.MaxFiles != defaultMaxFiles {
		t.Fatalf("MaxFiles = %d, want default %d", p.MaxFiles, defaultMaxFiles)
	}

	// Custom scrutiny + inert.
	if cs := p.classify(files(f("internal/auth/token.go", 5, 0))); !contains(cs.ScrutinyFlags, "auth-core") {
		t.Fatalf("scrutiny flags = %v, want auth-core", cs.ScrutinyFlags)
	}
	if ci := p.classify(files(f("README.md", 5, 0))); ci.Tier != "T0-trivial" {
		t.Fatalf("docs tier = %q, want T0-trivial", ci.Tier)
	}
}

func TestLoadPolicyInvalidPattern(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yml")
	if err := os.WriteFile(path, []byte("denyPaths:\n  - pattern: '['\n    category: x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPolicy(path); err == nil {
		t.Fatal("expected an error for an invalid regexp, got nil")
	}
}
