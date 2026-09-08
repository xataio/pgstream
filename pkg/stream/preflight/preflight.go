// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"encoding/json"
	"errors"
	"time"
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

// CheckStatus is the outcome the engine derived for a check. The engine always
// derives it; a check never sets its own status.
type CheckStatus string

const (
	StatusOK       CheckStatus = "ok"       // ran, found nothing wrong
	StatusFindings CheckStatus = "findings" // ran, reported at least one finding
	StatusError    CheckStatus = "error"    // ran, could not complete
	StatusNotRun   CheckStatus = "not_run"  // never started, or cut off before it produced a result
)

// StatusReason is a short machine-readable explanation of a status other than
// StatusOK.
type StatusReason string

const (
	ReasonFindingsReported      StatusReason = "findings_reported"       // accompanies StatusFindings
	ReasonCheckError            StatusReason = "check_error"             // accompanies StatusError; the message is in Err
	ReasonCheckDeadlineExceeded StatusReason = "check_deadline_exceeded" // exceeded the bound set with WithCheckTimeout
	ReasonRunDeadlineExceeded   StatusReason = "run_deadline_exceeded"   // the caller's context expired
	ReasonRunCanceled           StatusReason = "run_canceled"            // the caller cancelled the context
)

// Finding describes a single issue detected by a Check. Every finding is an
// error — a check that finds nothing wrong returns no findings at all.
//
// ID and Title identify the kind of problem, not the instance of it. Neither
// carries data read from the database under test, so a consumer can count
// findings by ID and show Title as a heading for a kind it has never seen.
// Detail carries the specifics: the tables, the version, the setting value.
// Message is the single line the CLI prints.
type Finding struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Detail  string `json:"detail"`
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

// CheckResult bundles a check's name with whatever it produced. Status and
// Reason are derived by the engine: Status says which of the four outcomes the
// check reached, and Reason explains every status other than StatusOK.
type CheckResult struct {
	Name     string         `json:"name"`
	Status   CheckStatus    `json:"-"`
	Reason   StatusReason   `json:"-"`
	Findings []Finding      `json:"findings"`
	Err      error          `json:"-"`
	Details  map[string]any `json:"-"`
	Summary  string         `json:"-"`
}

// resolve returns the result's status and reason. A result built outside the
// engine carries no status, so it is derived from the findings and the error
// and renders like one the engine produced.
func (r CheckResult) resolve() (CheckStatus, StatusReason) {
	if r.Status != "" {
		return r.Status, r.Reason
	}
	return deriveStatus(r.Findings, r.Err)
}

// deriveStatus classifies a check that returned. An error outranks findings,
// because a check that could not complete may have stopped part way through
// the work that produces them.
func deriveStatus(findings []Finding, err error) (CheckStatus, StatusReason) {
	switch {
	case err != nil:
		return StatusError, ReasonCheckError
	case len(findings) > 0:
		return StatusFindings, ReasonFindingsReported
	default:
		return StatusOK, ""
	}
}

// checkResultJSON is the wire shape of a CheckResult. Nesting Details lets a
// check name its keys freely, because those keys cannot collide with the
// engine's own fields. A struct also fixes the field order, which a map does
// not.
type checkResultJSON struct {
	Name     string         `json:"name"`
	Status   CheckStatus    `json:"status"`
	Reason   StatusReason   `json:"reason,omitempty"`
	Findings []Finding      `json:"findings"`
	Error    string         `json:"error,omitempty"`
	Details  map[string]any `json:"details,omitempty"`
}

// MarshalJSON renders Err as a string so the report is consumable from a
// non-Go process (the default error marshaling drops the message).
func (r CheckResult) MarshalJSON() ([]byte, error) {
	status, reason := r.resolve()
	out := checkResultJSON{
		Name:     r.Name,
		Status:   status,
		Reason:   reason,
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

// ProgressFunc is invoked just before each check runs. idx is 1-based. A check
// the engine does not start reports no progress.
type ProgressFunc func(idx, total int, name string)

// RunOption configures Run.
type RunOption func(*runOptions)

type runOptions struct {
	progress     ProgressFunc
	checkTimeout time.Duration
}

// WithProgress installs a callback invoked before each check runs. Useful for
// updating a spinner or log line with "running X of N: <name>".
func WithProgress(fn ProgressFunc) RunOption {
	return func(o *runOptions) { o.progress = fn }
}

// WithCheckTimeout bounds each check with a context deadline, so one slow
// check cannot consume the whole of the caller's budget. A check that exceeds
// the bound is reported as StatusNotRun, because a caller-imposed bound is not
// a defect in the check, and the run continues with the next check. The
// default is no bound, which runs every check to completion.
//
// The bound is the deadline the check is given, not a limit the engine
// enforces on its own: the engine runs each check to completion, one at a
// time. A check honours the bound by passing the context it is given to the
// work it does — every check in this package passes it to the driver, which is
// context-aware. A check that ignores its context still runs past the bound,
// and blocks the run while it does.
func WithCheckTimeout(d time.Duration) RunOption {
	return func(o *runOptions) { o.checkTimeout = d }
}

// Run executes every check in order. A check returning an error does not stop
// the run; subsequent checks still execute and the error is captured in the
// report alongside the findings. When the caller's context is done, Run starts
// no further checks and records each remaining one as StatusNotRun.
func Run(ctx context.Context, checks []Check, opts ...RunOption) Report {
	var ro runOptions
	for _, opt := range opts {
		opt(&ro)
	}

	results := make([]CheckResult, 0, len(checks))
	total := len(checks)
	for i, c := range checks {
		if err := ctx.Err(); err != nil {
			results = append(results, notRunResult(c.Name(), contextReason(err)))
			continue
		}
		if ro.progress != nil {
			ro.progress(i+1, total, c.Name())
		}
		results = append(results, ro.runCheck(ctx, c))
	}
	return Report{Results: results}
}

// runCheck executes one check under its own deadline and classifies what it
// produced. The check runs inline, so nothing of it outlives the call and the
// engine holds one check at a time — see WithCheckTimeout for what the bound
// does and does not promise.
func (o runOptions) runCheck(ctx context.Context, c Check) CheckResult {
	checkCtx := ctx
	if o.checkTimeout > 0 {
		var cancel context.CancelFunc
		checkCtx, cancel = context.WithTimeout(ctx, o.checkTimeout)
		defer cancel()
	}

	findings, err := c.Run(checkCtx)
	return collectResult(ctx, checkCtx, c, findings, err)
}

// collectResult builds the result of a check that returned, and reads its
// optional Details and Summary. A check whose context ended before it could
// produce a result did not run, whatever error it returned on the way out.
//
// That error is dropped rather than kept, because a check that noticed its
// context and one that failed on a dead connection report differently for the
// same cause. The reason says what stopped it, which is always knowable.
func collectResult(runCtx, checkCtx context.Context, c Check, findings []Finding, err error) CheckResult {
	if err != nil {
		if reason := cutoffReason(runCtx, checkCtx); reason != "" {
			return notRunResult(c.Name(), reason)
		}
	}

	status, reason := deriveStatus(findings, err)
	res := CheckResult{
		Name:     c.Name(),
		Status:   status,
		Reason:   reason,
		Findings: findings,
		Err:      err,
	}
	if d, ok := c.(Detailer); ok {
		res.Details = d.Details()
	}
	if s, ok := c.(Summarizer); ok {
		res.Summary = s.Summary()
	}
	return res
}

// cutoffReason reports why a check could not produce a result, or "" when
// neither context ended. It reads the run context first, so a run that ended
// is never reported as a per-check deadline.
func cutoffReason(runCtx, checkCtx context.Context) StatusReason {
	if err := runCtx.Err(); err != nil {
		return contextReason(err)
	}
	if checkCtx.Err() != nil {
		return ReasonCheckDeadlineExceeded
	}
	return ""
}

// contextReason distinguishes a run that ran out of time from one the caller
// cancelled.
func contextReason(err error) StatusReason {
	if errors.Is(err, context.DeadlineExceeded) {
		return ReasonRunDeadlineExceeded
	}
	return ReasonRunCanceled
}

func notRunResult(name string, reason StatusReason) CheckResult {
	return CheckResult{Name: name, Status: StatusNotRun, Reason: reason}
}

// HasErrors reports whether the run was anything other than clean: a check
// produced findings, a check failed to complete, or a check did not run. A
// check that did not run counts, because a report with missing checks is no
// evidence that the system is ready, and the CLI exit code must not claim it
// is. Callers that want only the checks which looked and objected must read
// the statuses in the report.
func (r Report) HasErrors() bool {
	for _, res := range r.Results {
		if status, _ := res.resolve(); status != StatusOK {
			return true
		}
	}
	return false
}
