// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"encoding/json"
)

// Category groups checks of the same concern so callers can opt in by
// category via CLI flags. New categories are added as new check sets land —
// see docs/migration_preflight_issue.md for the planned ones.
type Category string

const (
	CategoryConnectivity Category = "connectivity"
	CategoryReplication  Category = "replication"
	CategoryAccess       Category = "access"
	CategorySchema       Category = "schema"
	CategoryResources    Category = "resources"
)

// Finding describes a single issue detected by a Check. Every finding is an
// error — a check that finds nothing wrong returns no findings at all.
type Finding struct {
	Message string `json:"message"`
}

// Check is the minimal contract every preflight check must satisfy. Run returns
// the findings the check produced; a non-nil error means the check itself could
// not complete (distinct from finding a problem with the system under test).
type Check interface {
	Name() string
	Run(ctx context.Context) ([]Finding, error)
}

// Detailer is an optional interface. A Check implements it to attach
// structured, non-finding context to its result, for example the extensions it
// inspected. The engine calls Details after Run and puts the result in the JSON
// report only, under the "details" key.
type Detailer interface {
	Details() map[string]any
}

// Summarizer is an optional interface. A Check implements it to report one
// short line about what it observed, such as a size or a version. The engine
// calls Summary after Run and prints the result next to the check name in the
// human-readable report only.
type Summarizer interface {
	Summary() string
}

// CheckResult bundles a check's name with whatever it produced.
type CheckResult struct {
	Name     string         `json:"name"`
	Findings []Finding      `json:"findings"`
	Err      error          `json:"-"`
	Details  map[string]any `json:"-"`
	Summary  string         `json:"-"`
}

// checkResultJSON is the wire shape of a CheckResult. Nesting Details lets a
// check name its keys freely, because those keys cannot collide with the
// engine's own fields. A struct also fixes the field order, which a map does
// not.
type checkResultJSON struct {
	Name     string         `json:"name"`
	Findings []Finding      `json:"findings"`
	Error    string         `json:"error,omitempty"`
	Details  map[string]any `json:"details,omitempty"`
}

// MarshalJSON renders Err as a string so the report is consumable from a
// non-Go process (the default error marshaling drops the message).
func (r CheckResult) MarshalJSON() ([]byte, error) {
	out := checkResultJSON{
		Name:     r.Name,
		Findings: r.Findings,
		Details:  r.Details,
	}
	if r.Err != nil {
		out.Error = r.Err.Error()
	}
	return json.Marshal(out)
}

// Report is the outcome of running a set of checks.
type Report struct {
	Results []CheckResult `json:"results"`
}

// ProgressFunc is invoked just before each check runs. idx is 1-based.
type ProgressFunc func(idx, total int, name string)

// RunOption configures Run.
type RunOption func(*runOptions)

type runOptions struct {
	progress ProgressFunc
}

// WithProgress installs a callback invoked before each check runs. Useful for
// updating a spinner or log line with "running X of N: <name>".
func WithProgress(fn ProgressFunc) RunOption {
	return func(o *runOptions) { o.progress = fn }
}

// Run executes every check in order. A check returning an error does not stop
// the run; subsequent checks still execute and the error is captured in the
// report alongside the findings.
func Run(ctx context.Context, checks []Check, opts ...RunOption) Report {
	var ro runOptions
	for _, opt := range opts {
		opt(&ro)
	}

	results := make([]CheckResult, 0, len(checks))
	total := len(checks)
	for i, c := range checks {
		if ro.progress != nil {
			ro.progress(i+1, total, c.Name())
		}
		findings, err := c.Run(ctx)
		res := CheckResult{
			Name:     c.Name(),
			Findings: findings,
			Err:      err,
		}
		if d, ok := c.(Detailer); ok {
			res.Details = d.Details()
		}
		if s, ok := c.(Summarizer); ok {
			res.Summary = s.Summary()
		}
		results = append(results, res)
	}
	return Report{Results: results}
}

// HasErrors reports whether any check produced findings or failed to complete.
func (r Report) HasErrors() bool {
	for _, res := range r.Results {
		if res.Err != nil || len(res.Findings) > 0 {
			return true
		}
	}
	return false
}
