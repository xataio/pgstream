// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

// Configurable gate policy. There is no policy hardcoded in Go: pgstream's policy
// lives in policy.yml, loaded at runtime via --config (which defaults to
// <agent-dir>/policy.yml). Other repos supply their own, so the agent can be
// reused without code changes. review-guidance.md (the LLM's instructions) is a
// separate file whose path is set with --guidance.

const (
	defaultMaxLines = 300
	defaultMaxFiles = 15
)

type pathRule struct {
	pattern  *regexp.Regexp
	category string
}

// policy is the compiled, runtime form of the gate configuration.
type policy struct {
	DenyPaths     []pathRule       // always ESCALATE to a human
	ScrutinyPaths []pathRule       // reviewed, but the model leans toward ESCALATE
	InertPaths    []*regexp.Regexp // auto-approvable without an LLM; kept across in dismiss
	TrivialPaths  []*regexp.Regexp // size-exempt (but still reviewed unless also inert)
	MaxLines      int
	MaxFiles      int
}

func (p *policy) isTrivial(path string) bool { return matchAny(p.TrivialPaths, path) }
func (p *policy) isInert(path string) bool   { return matchAny(p.InertPaths, path) }

func matchAny(res []*regexp.Regexp, path string) bool {
	for _, re := range res {
		if re.MatchString(path) {
			return true
		}
	}
	return false
}

// --- YAML config schema (as authored by a repo) -----------------------------

type ruleConfig struct {
	Pattern  string `yaml:"pattern"`
	Category string `yaml:"category"` // deny category, or scrutiny flag
}

type policyConfig struct {
	DenyPaths           []ruleConfig `yaml:"denyPaths"`
	ScrutinyPaths       []ruleConfig `yaml:"scrutinyPaths"`
	InertPaths          []string     `yaml:"inertPaths"`
	TrivialPaths        []string     `yaml:"trivialPaths"`
	MaxSubstantiveLines int          `yaml:"maxSubstantiveLines"`
	MaxSubstantiveFiles int          `yaml:"maxSubstantiveFiles"`
}

// loadPolicy reads and compiles a policy from a YAML (or JSON) file.
func loadPolicy(path string) (*policy, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading policy %s: %w", path, err)
	}
	p, err := parsePolicy(b)
	if err != nil {
		return nil, fmt.Errorf("parsing policy %s: %w", path, err)
	}
	return p, nil
}

// parsePolicy compiles a policy from YAML (or JSON) bytes. Omitted path lists
// become empty; omitted thresholds fall back to the built-in defaults.
func parsePolicy(b []byte) (*policy, error) {
	var cfg policyConfig
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}

	var err error
	p := &policy{MaxLines: cfg.MaxSubstantiveLines, MaxFiles: cfg.MaxSubstantiveFiles}
	if p.MaxLines <= 0 {
		p.MaxLines = defaultMaxLines
	}
	if p.MaxFiles <= 0 {
		p.MaxFiles = defaultMaxFiles
	}
	if p.DenyPaths, err = compileRules("denyPaths", cfg.DenyPaths); err != nil {
		return nil, err
	}
	if p.ScrutinyPaths, err = compileRules("scrutinyPaths", cfg.ScrutinyPaths); err != nil {
		return nil, err
	}
	if p.InertPaths, err = compilePatterns("inertPaths", cfg.InertPaths); err != nil {
		return nil, err
	}
	if p.TrivialPaths, err = compilePatterns("trivialPaths", cfg.TrivialPaths); err != nil {
		return nil, err
	}
	return p, nil
}

func compileRules(field string, rc []ruleConfig) ([]pathRule, error) {
	out := make([]pathRule, 0, len(rc))
	for _, r := range rc {
		re, err := regexp.Compile(r.Pattern)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid pattern %q: %w", field, r.Pattern, err)
		}
		out = append(out, pathRule{pattern: re, category: r.Category})
	}
	return out, nil
}

func compilePatterns(field string, pats []string) ([]*regexp.Regexp, error) {
	out := make([]*regexp.Regexp, 0, len(pats))
	for _, pat := range pats {
		re, err := regexp.Compile(pat)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid pattern %q: %w", field, pat, err)
		}
		out = append(out, re)
	}
	return out, nil
}
