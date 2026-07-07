// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

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

func TestCheckResult_JSONMarshalMergesDetails(t *testing.T) {
	t.Parallel()

	res := CheckResult{
		Name:    "schema_extension_compatibility",
		Details: map[string]any{"source_extensions": []string{"hstore", "postgis"}},
	}

	data, err := json.Marshal(res)
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"schema_extension_compatibility","findings":null,"source_extensions":["hstore","postgis"]}`, string(data))
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
				Name:     "a",
				Findings: []Finding{{Message: "broken"}},
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
		`{"name":"a","findings":[{"message":"broken"}]},` +
		`{"name":"b","findings":null,"error":"boom"}` +
		`]}`
	require.JSONEq(t, expected, string(data))
}
