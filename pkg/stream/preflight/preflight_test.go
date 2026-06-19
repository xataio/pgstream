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
		&stubCheck{name: "b", ran: &ranB, findings: []Finding{{Severity: SeverityError, Message: "broken"}}},
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
	require.Equal(t, []Finding{{Severity: SeverityError, Message: "broken"}}, report.Results[1].Findings)

	require.Equal(t, "c", report.Results[2].Name)
	require.NoError(t, report.Results[2].Err)
	require.Empty(t, report.Results[2].Findings)

	require.True(t, report.HasErrors())
}

func TestRun_HasErrorsFalseWhenOnlyWarningsAndInfo(t *testing.T) {
	t.Parallel()

	checks := []Check{
		&stubCheck{name: "warn", findings: []Finding{{Severity: SeverityWarning, Message: "heads up"}}},
		&stubCheck{name: "info", findings: []Finding{{Severity: SeverityInfo, Message: "fyi"}}},
	}

	report := Run(context.Background(), checks)

	require.False(t, report.HasErrors())
}

func TestRun_EmptyChecksProducesEmptyReport(t *testing.T) {
	t.Parallel()

	report := Run(context.Background(), nil)

	require.Empty(t, report.Results)
	require.False(t, report.HasErrors())
}

func TestReport_PrettyPrint_InfoOnlyShowsAsPassed(t *testing.T) {
	t.Parallel()

	report := Report{
		Results: []CheckResult{
			{Name: "clean"},
			{Name: "info-only", Findings: []Finding{{Severity: SeverityInfo, Message: "fyi"}}},
			{Name: "with-warning", Findings: []Finding{{Severity: SeverityWarning, Message: "heads up"}}},
		},
	}

	out := report.PrettyPrint()

	require.Contains(t, out, "✔ clean\n")
	require.Contains(t, out, "✔ info-only (with notes)\n")
	require.Contains(t, out, "ℹ info-only: fyi\n")
	require.NotContains(t, out, "✔ with-warning")
	require.Contains(t, out, "⚠ with-warning: heads up\n")
}

func TestReport_JSONMarshal(t *testing.T) {
	t.Parallel()

	report := Report{
		Results: []CheckResult{
			{
				Name:     "a",
				Findings: []Finding{{Severity: SeverityError, Message: "broken"}},
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
		`{"name":"a","findings":[{"severity":"error","message":"broken"}]},` +
		`{"name":"b","findings":null,"error":"boom"}` +
		`]}`
	require.JSONEq(t, expected, string(data))
}
