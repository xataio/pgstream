// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"path"
	"strings"
)

// Deterministic gates for the PR approval agent.
//
// Gates are authoritative: they run before the LLM and the LLM can only tighten
// their verdict, never loosen it. A path that lands in the deny-list is escalated
// to a human regardless of what the model thinks of the code.
//
// The path lists and thresholds come from a policy (see config.go), loaded from a
// YAML file at runtime — pgstream's policy.yml or a repo-supplied --config file.

type changedFile struct {
	Path      string `json:"path"`
	Prev      string `json:"prev,omitempty"` // previous path for a rename, if any
	Additions int    `json:"additions"`
	Deletions int    `json:"deletions"`
}

type classification struct {
	Tier             string   `json:"tier"` // "T0-trivial" | "T1-agent" | "T2-never"
	DenyCategories   []string `json:"deny_categories"`
	ScrutinyFlags    []string `json:"scrutiny_flags"`
	SubstantiveLines int      `json:"substantive_lines"`
	SubstantiveFiles int      `json:"substantive_files"`
	TrivialOnly      bool     `json:"trivial_only"`
}

type gateResult struct {
	Allow   bool     `json:"allow"`   // may the LLM even be consulted?
	Verdict string   `json:"verdict"` // "PASS" | "ESCALATE"
	Reasons []string `json:"reasons"`
}

func normPath(p string) string {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "./")
	return path.Clean(p)
}

func appendUnique(list []string, v string) []string {
	for _, existing := range list {
		if existing == v {
			return list
		}
	}
	return append(list, v)
}

// classify deterministically categorises a set of changed files against the
// policy.
//
// Ordering matters: deny wins over everything (a single dangerous file taints the
// whole PR), then inert-only (auto-approvable), then default agent review.
func (p *policy) classify(files []changedFile) classification {
	c := classification{
		DenyCategories: []string{},
		ScrutinyFlags:  []string{},
		TrivialOnly:    true,
	}
	// A PR auto-approves without an LLM only when every touched path (new AND, for
	// renames, old) is inert. An empty file list is never inert.
	inertOnly := len(files) > 0

	for _, f := range files {
		fp := normPath(f.Path)

		// Match deny/scrutiny against both the new and the pre-rename path, so
		// moving a sensitive file out of (or into) a gated directory still trips
		// the gate instead of showing up as a single trivial entry.
		paths := []string{fp}
		if f.Prev != "" {
			paths = append(paths, normPath(f.Prev))
		}
		for _, pp := range paths {
			for _, rule := range p.DenyPaths {
				if rule.pattern.MatchString(pp) {
					c.DenyCategories = appendUnique(c.DenyCategories, rule.category)
				}
			}
			for _, rule := range p.ScrutinyPaths {
				if rule.pattern.MatchString(pp) {
					c.ScrutinyFlags = appendUnique(c.ScrutinyFlags, rule.category)
				}
			}
			if !p.isInert(pp) {
				inertOnly = false
			}
		}

		if p.isTrivial(fp) {
			continue
		}
		c.TrivialOnly = false
		c.SubstantiveFiles++
		c.SubstantiveLines += f.Additions + f.Deletions
	}

	switch {
	case len(c.DenyCategories) > 0:
		c.Tier = "T2-never"
		c.TrivialOnly = false
	case inertOnly:
		c.Tier = "T0-trivial"
	default:
		c.Tier = "T1-agent"
	}

	return c
}

// runGates applies the authoritative gates to a classification. Allow=false means
// the PR must not be auto-approved. Allow=true means the LLM may be consulted (or,
// for T0-trivial, auto-approval is permitted).
func (p *policy) runGates(c classification, mergeable string) gateResult {
	if mergeable == "CONFLICTING" {
		return gateResult{
			Allow:   false,
			Verdict: "ESCALATE",
			Reasons: []string{"PR has merge conflicts; rebase before review"},
		}
	}
	if len(c.DenyCategories) > 0 {
		return gateResult{
			Allow:   false,
			Verdict: "ESCALATE",
			Reasons: []string{"touches deny-listed area(s) requiring human review: " + strings.Join(c.DenyCategories, ", ")},
		}
	}
	if c.SubstantiveLines > p.MaxLines {
		return gateResult{
			Allow:   false,
			Verdict: "ESCALATE",
			Reasons: []string{fmt.Sprintf("%d substantive lines exceeds the %d-line ceiling for auto-review", c.SubstantiveLines, p.MaxLines)},
		}
	}
	if c.SubstantiveFiles > p.MaxFiles {
		return gateResult{
			Allow:   false,
			Verdict: "ESCALATE",
			Reasons: []string{fmt.Sprintf("%d substantive files exceeds the %d-file ceiling for auto-review", c.SubstantiveFiles, p.MaxFiles)},
		}
	}

	reasons := []string{}
	if len(c.ScrutinyFlags) > 0 {
		reasons = append(reasons, "scrutiny areas touched: "+strings.Join(c.ScrutinyFlags, ", "))
	}
	return gateResult{Allow: true, Verdict: "PASS", Reasons: reasons}
}
