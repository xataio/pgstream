// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type stubCheck struct {
	name     string
	findings []Finding
	err      error
	ran      *bool
}

func (s *stubCheck) Name() string { return s.name }

func (s *stubCheck) Run(_ context.Context) ([]Finding, error) {
	if s.ran != nil {
		*s.ran = true
	}
	return s.findings, s.err
}

func TestRun_RunsAllChecksEvenWhenSomeFail(t *testing.T) {
	t.Parallel()

	ranA, ranB, ranC := false, false, false
	checkErr := errors.New("boom")

	checks := []Check{
		&stubCheck{name: "a", ran: &ranA, err: checkErr},
		&stubCheck{name: "b", ran: &ranB, findings: []Finding{{Message: "broken"}}},
		&stubCheck{name: "c", ran: &ranC},
	}

	report := Run(context.Background(), checks)

	require.True(t, ranA, "check a should have run")
	require.True(t, ranB, "check b should have run despite check a's error")
	require.True(t, ranC, "check c should have run despite check b's finding")

	require.Len(t, report.Results, 3)

	require.Equal(t, "a", report.Results[0].Name)
	require.ErrorIs(t, report.Results[0].Err, checkErr)
	require.Empty(t, report.Results[0].Findings)

	require.Equal(t, "b", report.Results[1].Name)
	require.NoError(t, report.Results[1].Err)
	require.Equal(t, []Finding{{Message: "broken"}}, report.Results[1].Findings)

	require.Equal(t, "c", report.Results[2].Name)
	require.NoError(t, report.Results[2].Err)
	require.Empty(t, report.Results[2].Findings)

	require.True(t, report.HasErrors())
}

func TestRun_EmptyChecksProducesEmptyReport(t *testing.T) {
	t.Parallel()

	report := Run(context.Background(), nil)

	require.Empty(t, report.Results)
	require.False(t, report.HasErrors())
}

func TestReportPrinter_PrettyPrint(t *testing.T) {
	t.Parallel()

	printer := ReportPrinter{Report: Report{
		Results: []CheckResult{
			{Name: "clean"},
			{Name: "with-findings", Findings: []Finding{{Message: "broken"}}},
			{Name: "check-failed", Err: errors.New("boom")},
		},
	}}

	out := printer.PrettyPrint()

	require.Contains(t, out, "✔ clean\n")
	require.Contains(t, out, "✘ with-findings: broken\n")
	require.Contains(t, out, "✘ check-failed: check failed: boom\n")
	require.Contains(t, out, "ran 3 checks\n")
}

type stubDetailerCheck struct {
	stubCheck
	details map[string]any
}

func (s *stubDetailerCheck) Details() map[string]any { return s.details }

func TestRun_CapturesDetailsFromDetailerChecks(t *testing.T) {
	t.Parallel()

	checks := []Check{
		&stubDetailerCheck{
			stubCheck: stubCheck{name: "with-details"},
			details:   map[string]any{"source_extensions": []string{"hstore", "postgis"}},
		},
		&stubCheck{name: "no-details"},
	}

	report := Run(context.Background(), checks)

	require.Equal(t, map[string]any{"source_extensions": []string{"hstore", "postgis"}}, report.Results[0].Details)
	require.Nil(t, report.Results[1].Details)
}

func TestCheckResult_JSONNestsDetails(t *testing.T) {
	t.Parallel()

	res := CheckResult{
		Name:    "schema_extension_compatibility",
		Details: map[string]any{"source_extensions": []string{"hstore", "postgis"}},
	}

	data, err := json.Marshal(res)
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"schema_extension_compatibility","status":"ok","findings":null,"details":{"source_extensions":["hstore","postgis"]}}`, string(data))
}

func TestReportPrinter_PrettyPrintOmitsDetails(t *testing.T) {
	t.Parallel()

	printer := ReportPrinter{Report: Report{
		Results: []CheckResult{
			{Name: "clean", Details: map[string]any{"source_extensions": []string{"hstore", "postgis"}}},
		},
	}}

	out := printer.PrettyPrint()

	// details are JSON-only; the human report never mentions them
	require.Equal(t, "✔ clean\nran 1 checks\n", out)
}

type stubSummarizerCheck struct {
	stubCheck
	summary string
}

func (s *stubSummarizerCheck) Summary() string { return s.summary }

func TestRun_CapturesSummariesFromSummarizerChecks(t *testing.T) {
	t.Parallel()

	checks := []Check{
		&stubSummarizerCheck{
			stubCheck: stubCheck{name: "database_size"},
			summary:   "12 MB",
		},
		&stubCheck{name: "no-summary"},
	}

	report := Run(context.Background(), checks)

	require.Equal(t, "12 MB", report.Results[0].Summary)
	require.Empty(t, report.Results[1].Summary)
}

func TestCheckResult_JSONOmitsSummary(t *testing.T) {
	t.Parallel()

	res := CheckResult{
		Name:    "database_size",
		Summary: "12 MB",
		Details: map[string]any{"database_size_bytes": int64(12582912)},
	}

	data, err := json.Marshal(res)
	require.NoError(t, err)
	// JSON carries the typed Details, never the rendered summary string
	require.JSONEq(t, `{"name":"database_size","status":"ok","findings":null,"details":{"database_size_bytes":12582912}}`, string(data))
}

func TestReportPrinter_PrettyPrintSummaries(t *testing.T) {
	t.Parallel()

	report := Report{
		Results: []CheckResult{
			{Name: "wal_level"},
			{Name: "postgres_version", Summary: "16.4"},
			{Name: "database_size", Summary: "12 MB"},
		},
	}

	out := ReportPrinter{Report: report}.PrettyPrint()

	require.Equal(t, strings.Join([]string{
		"✔ wal_level",
		"✔ postgres_version  16.4",
		"✔ database_size     12 MB",
		"ran 3 checks",
		"",
	}, "\n"), out)
}

func TestReportPrinter_PrettyPrintSkipsSummaryOnFailure(t *testing.T) {
	t.Parallel()

	printer := ReportPrinter{Report: Report{Results: []CheckResult{
		{
			Name:     "postgres_version",
			Summary:  "source 16.4 → target 15.2",
			Findings: []Finding{{Message: "downgrade"}},
		},
		{Name: "database_size", Summary: "never read", Err: errors.New("boom")},
	}}}

	out := printer.PrettyPrint()

	// the finding is what the reader needs; the summary never reaches a ✘ line
	require.Equal(t, "✘ postgres_version: downgrade\n✘ database_size: check failed: boom\nran 2 checks\n", out)
}

func TestReportPrinter_MarshalJSONDelegatesToReport(t *testing.T) {
	t.Parallel()

	report := Report{
		Results: []CheckResult{
			{Name: "a", Findings: []Finding{{Message: "broken"}}},
		},
	}

	viaReport, err := json.Marshal(report)
	require.NoError(t, err)
	viaPrinter, err := json.Marshal(ReportPrinter{Report: report})
	require.NoError(t, err)

	require.JSONEq(t, string(viaReport), string(viaPrinter))
}

func TestReport_JSONMarshal(t *testing.T) {
	t.Parallel()

	report := Report{
		Results: []CheckResult{
			{
				Name: "a",
				Findings: []Finding{{
					ID:      "wal_level_not_logical",
					Title:   "The source wal_level is not logical",
					Detail:  "The source runs with wal_level=\"replica\".",
					Message: "broken",
				}},
			},
			{
				Name: "b",
				Err:  errors.New("boom"),
			},
		},
	}

	data, err := json.Marshal(report)
	require.NoError(t, err)

	expected := `{"results":[` +
		`{"name":"a","status":"findings","reason":"findings_reported","findings":[{` +
		`"id":"wal_level_not_logical",` +
		`"title":"The source wal_level is not logical",` +
		`"detail":"The source runs with wal_level=\"replica\".",` +
		`"message":"broken"}]},` +
		`{"name":"b","status":"error","reason":"check_error","findings":null,"error":"boom"}` +
		`]}`
	require.JSONEq(t, expected, string(data))
}

// countingCheck records how many times the engine invoked it.
type countingCheck struct {
	name  string
	calls atomic.Int64
}

func (c *countingCheck) Name() string { return c.name }

func (c *countingCheck) Run(_ context.Context) ([]Finding, error) {
	c.calls.Add(1)
	return nil, nil
}

// blockingCheck ignores its context and only returns when the test releases
// it. It records whether the engine asked it for a summary, so a test can
// assert that the engine read nothing from a check it abandoned.
type blockingCheck struct {
	name         string
	started      chan struct{}
	release      chan struct{}
	done         chan struct{}
	summary      string
	summaryAsked atomic.Bool
}

func newBlockingCheck(name string) *blockingCheck {
	return &blockingCheck{
		name:    name,
		started: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (b *blockingCheck) Name() string { return b.name }

func (b *blockingCheck) Run(_ context.Context) ([]Finding, error) {
	close(b.started)
	defer close(b.done)
	<-b.release
	b.summary = "written after the deadline"
	return nil, nil
}

func (b *blockingCheck) Summary() string {
	b.summaryAsked.Store(true)
	return b.summary
}

// releaseAndWait unblocks the check and waits for it, so the goroutine the
// engine abandoned is finished before the test ends.
func (b *blockingCheck) releaseAndWait() {
	close(b.release)
	<-b.done
}

func TestRun_CheckTimeoutReportsBlockedCheckAsNotRun(t *testing.T) {
	t.Parallel()

	blocked := newBlockingCheck("blocked")
	defer blocked.releaseAndWait()
	next := &countingCheck{name: "next"}

	report := Run(context.Background(), []Check{blocked, next}, WithCheckTimeout(50*time.Millisecond))

	require.Len(t, report.Results, 2)
	require.Equal(t, "blocked", report.Results[0].Name)
	require.Equal(t, StatusNotRun, report.Results[0].Status)
	require.Equal(t, ReasonCheckDeadlineExceeded, report.Results[0].Reason)
	require.NoError(t, report.Results[0].Err)
	require.False(t, blocked.summaryAsked.Load(), "an abandoned check must not be read for its summary")
	require.Empty(t, report.Results[0].Summary)

	require.Equal(t, "next", report.Results[1].Name)
	require.Equal(t, StatusOK, report.Results[1].Status)
	require.Equal(t, int64(1), next.calls.Load(), "the check after the bounded one should still run")

	require.True(t, report.HasErrors())
}

// deadlineAwareCheck honours its context and reports the context error, which
// is how a well-behaved check reacts to a bound it cannot meet.
type deadlineAwareCheck struct {
	name string
}

func (d *deadlineAwareCheck) Name() string { return d.name }

func (d *deadlineAwareCheck) Run(ctx context.Context) ([]Finding, error) {
	<-ctx.Done()
	return nil, fmt.Errorf("querying source: %w", ctx.Err())
}

func TestRun_CheckTimeoutReportsContextAwareCheckAsNotRun(t *testing.T) {
	t.Parallel()

	checks := []Check{&deadlineAwareCheck{name: "slow"}, &countingCheck{name: "next"}}

	report := Run(context.Background(), checks, WithCheckTimeout(10*time.Millisecond))

	require.Equal(t, StatusNotRun, report.Results[0].Status)
	require.Equal(t, ReasonCheckDeadlineExceeded, report.Results[0].Reason)
	require.Equal(t, StatusOK, report.Results[1].Status)
}

// partialCheck reports what it found before its context ended, which is how a
// check that walks a catalog reacts to a bound it cannot meet.
type partialCheck struct {
	name string
}

func (p *partialCheck) Name() string { return p.name }

func (p *partialCheck) Run(ctx context.Context) ([]Finding, error) {
	<-ctx.Done()
	return []Finding{{Message: "public.orders has no replica identity"}}, fmt.Errorf("walking pg_class: %w", ctx.Err())
}

// TestRun_CutOffCheckReportsTheSameResultEveryRun pins why a cut off check
// keeps neither its error nor its findings. Whether the check delivers them
// before the engine stops waiting is a scheduling race, so reporting them
// would make the same check under the same bound report differently from run
// to run.
func TestRun_CutOffCheckReportsTheSameResultEveryRun(t *testing.T) {
	t.Parallel()

	for range 50 {
		report := Run(context.Background(), []Check{&partialCheck{name: "replica_identity"}}, WithCheckTimeout(time.Millisecond))

		res := report.Results[0]
		require.Equal(t, StatusNotRun, res.Status)
		require.Equal(t, ReasonCheckDeadlineExceeded, res.Reason)
		require.Empty(t, res.Findings)
		require.NoError(t, res.Err)
	}
}

// stubReportingCheck implements both optional interfaces, so a test can prove
// the engine still reads them for a check that returned within its bound.
type stubReportingCheck struct {
	stubCheck
	details map[string]any
	summary string
}

func (s *stubReportingCheck) Details() map[string]any { return s.details }

func (s *stubReportingCheck) Summary() string { return s.summary }

func TestRun_CheckTimeoutCollectsChecksThatFinishWithinTheBound(t *testing.T) {
	t.Parallel()

	checkErr := errors.New("boom")
	checks := []Check{
		&stubReportingCheck{
			stubCheck: stubCheck{name: "clean"},
			details:   map[string]any{"tables": 3},
			summary:   "3 tables",
		},
		&stubCheck{name: "with-findings", findings: []Finding{{Message: "broken"}}},
		&stubCheck{name: "failed", err: checkErr},
	}

	report := Run(context.Background(), checks, WithCheckTimeout(time.Minute))

	require.Equal(t, StatusOK, report.Results[0].Status)
	require.Empty(t, report.Results[0].Reason)
	require.Equal(t, map[string]any{"tables": 3}, report.Results[0].Details)
	require.Equal(t, "3 tables", report.Results[0].Summary)

	require.Equal(t, StatusFindings, report.Results[1].Status)
	require.Equal(t, ReasonFindingsReported, report.Results[1].Reason)
	require.Equal(t, []Finding{{Message: "broken"}}, report.Results[1].Findings)

	require.Equal(t, StatusError, report.Results[2].Status)
	require.Equal(t, ReasonCheckError, report.Results[2].Reason)
	require.ErrorIs(t, report.Results[2].Err, checkErr)
}

func TestRun_RunDeadlineDuringBoundedCheckOutranksItsBound(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	blocked := newBlockingCheck("blocked")
	defer blocked.releaseAndWait()
	next := &countingCheck{name: "next"}

	report := Run(ctx, []Check{blocked, next}, WithCheckTimeout(time.Minute))

	require.Equal(t, StatusNotRun, report.Results[0].Status)
	require.Equal(t, ReasonRunDeadlineExceeded, report.Results[0].Reason,
		"a run that ended is never reported as a per-check deadline")
	require.Equal(t, StatusNotRun, report.Results[1].Status)
	require.Equal(t, ReasonRunDeadlineExceeded, report.Results[1].Reason)
	require.Zero(t, next.calls.Load())
}

func TestRun_CancellationDuringBoundedCheckIsDistinguishableFromItsBound(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	blocked := newBlockingCheck("blocked")
	defer blocked.releaseAndWait()
	next := &countingCheck{name: "next"}

	go func() {
		<-blocked.started
		cancel()
	}()

	report := Run(ctx, []Check{blocked, next}, WithCheckTimeout(time.Minute))

	require.Equal(t, StatusNotRun, report.Results[0].Status)
	require.Equal(t, ReasonRunCanceled, report.Results[0].Reason)
	require.Equal(t, StatusNotRun, report.Results[1].Status)
	require.Equal(t, ReasonRunCanceled, report.Results[1].Reason)
	require.Zero(t, next.calls.Load(), "a check after a cancelled run must never be invoked")
}

// cancellingCheck ends the run from inside a check, which is how a caller's
// cancellation arrives mid-run.
type cancellingCheck struct {
	name   string
	cancel context.CancelFunc
}

func (c *cancellingCheck) Name() string { return c.name }

func (c *cancellingCheck) Run(_ context.Context) ([]Finding, error) {
	c.cancel()
	return nil, nil
}

func TestRun_CancelledContextSkipsRemainingChecks(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := &cancellingCheck{name: "first", cancel: cancel}
	second := &countingCheck{name: "second"}
	third := &countingCheck{name: "third"}

	report := Run(ctx, []Check{first, second, third})

	require.Len(t, report.Results, 3)
	require.Equal(t, StatusOK, report.Results[0].Status)

	for _, res := range report.Results[1:] {
		require.Equal(t, StatusNotRun, res.Status)
		require.Equal(t, ReasonRunCanceled, res.Reason)
		require.Empty(t, res.Findings)
		require.NoError(t, res.Err)
	}

	require.Zero(t, second.calls.Load(), "a check after a cancelled run must never be invoked")
	require.Zero(t, third.calls.Load(), "a check after a cancelled run must never be invoked")
	require.True(t, report.HasErrors())
}

func TestRun_ExpiredDeadlineIsDistinguishableFromCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	check := &countingCheck{name: "never-invoked"}

	report := Run(ctx, []Check{check})

	require.Len(t, report.Results, 1)
	require.Equal(t, StatusNotRun, report.Results[0].Status)
	require.Equal(t, ReasonRunDeadlineExceeded, report.Results[0].Reason)
	require.Zero(t, check.calls.Load())
}

// contextReportingCheck reports whether the engine gave it a bounded context.
type contextReportingCheck struct {
	name        string
	hasDeadline bool
}

func (c *contextReportingCheck) Name() string { return c.name }

func (c *contextReportingCheck) Run(ctx context.Context) ([]Finding, error) {
	_, c.hasDeadline = ctx.Deadline()
	return nil, nil
}

func TestRun_WithoutCheckTimeoutLeavesChecksUnbounded(t *testing.T) {
	t.Parallel()

	reporting := &contextReportingCheck{name: "reporting"}
	blocked := newBlockingCheck("slow")
	go func() {
		time.Sleep(10 * time.Millisecond)
		blocked.releaseAndWait()
	}()

	report := Run(context.Background(), []Check{reporting, blocked})

	require.False(t, reporting.hasDeadline, "an unbounded run passes the caller's context untouched")
	require.Equal(t, StatusOK, report.Results[1].Status, "an unbounded check runs to completion")
	require.Equal(t, "written after the deadline", report.Results[1].Summary)
	require.False(t, report.HasErrors())
}

func TestRun_DerivesStatusForEveryOutcome(t *testing.T) {
	t.Parallel()

	checks := []Check{
		&stubCheck{name: "clean"},
		&stubCheck{name: "with-findings", findings: []Finding{{Message: "broken"}}},
		&stubCheck{name: "failed", err: errors.New("boom")},
	}

	report := Run(context.Background(), checks)

	require.Equal(t, StatusOK, report.Results[0].Status)
	require.Empty(t, report.Results[0].Reason)
	require.Equal(t, StatusFindings, report.Results[1].Status)
	require.Equal(t, ReasonFindingsReported, report.Results[1].Reason)
	require.Equal(t, StatusError, report.Results[2].Status)
	require.Equal(t, ReasonCheckError, report.Results[2].Reason)
}

func TestReport_HasErrorsCountsNotRunChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		results []CheckResult
		want    bool
	}{
		{
			name:    "every check ran and passed",
			results: []CheckResult{{Name: "a", Status: StatusOK}, {Name: "b", Status: StatusOK}},
			want:    false,
		},
		{
			name:    "a check did not run",
			results: []CheckResult{{Name: "a", Status: StatusOK}, {Name: "b", Status: StatusNotRun, Reason: ReasonCheckDeadlineExceeded}},
			want:    true,
		},
		{
			name:    "results built without a status keep the old meaning",
			results: []CheckResult{{Name: "a"}, {Name: "b", Findings: []Finding{{Message: "broken"}}}},
			want:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, Report{Results: tc.results}.HasErrors())
		})
	}
}

func TestCheckResult_JSONCarriesStatusAndReason(t *testing.T) {
	t.Parallel()

	res := CheckResult{Name: "database_size", Status: StatusNotRun, Reason: ReasonCheckDeadlineExceeded}

	data, err := json.Marshal(res)
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"database_size","status":"not_run","reason":"check_deadline_exceeded","findings":null}`, string(data))
}

func TestReportPrinter_PrettyPrintNotRun(t *testing.T) {
	t.Parallel()

	printer := ReportPrinter{Report: Report{Results: []CheckResult{
		{Name: "wal_level", Status: StatusOK, Summary: "logical"},
		{Name: "database_size", Status: StatusNotRun, Reason: ReasonCheckDeadlineExceeded},
		{Name: "table_access", Status: StatusNotRun, Reason: ReasonRunCanceled},
	}}}

	out := printer.PrettyPrint()

	require.Equal(t, strings.Join([]string{
		"✔ wal_level  logical",
		"⊘ database_size: did not run (check deadline exceeded)",
		"⊘ table_access: did not run (run canceled)",
		"ran 1 of 3 checks, 2 did not run",
		"",
	}, "\n"), out)
}
