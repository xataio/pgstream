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

// Detailer is an optional interface a Check may implement to attach structured,
// non-finding context to its result (e.g. the set of extensions it inspected).
// The engine calls Details after Run, so a check populates it from state it
// gathered while running. Each key is merged into the check's JSON object; it is
// only surfaced in the JSON report, not the human-readable one.
type Detailer interface {
	Details() map[string]any
}

// CheckResult bundles a check's name with whatever it produced.
type CheckResult struct {
	Name     string    `json:"name"`
	Findings []Finding `json:"findings"`
	Err      error     `json:"-"`
	// Details holds optional structured context from a Detailer check. Keys are
	// merged into the result's JSON object alongside name/findings/error.
	Details map[string]any `json:"-"`
}

// MarshalJSON renders Err as a string so the report is consumable from a
// non-Go process (the default error marshaling drops the message), and merges
// any Details keys into the object.
func (r CheckResult) MarshalJSON() ([]byte, error) {
	out := map[string]any{
		"name":     r.Name,
		"findings": r.Findings,
	}
	if r.Err != nil {
		out["error"] = r.Err.Error()
	}
	for k, v := range r.Details {
		if _, taken := out[k]; !taken {
			out[k] = v
		}
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
