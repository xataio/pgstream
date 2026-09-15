// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// ReportPrinter renders a Report for display. It satisfies the cmd-side
// printer contract (PrettyPrint string + json.Marshaler), so existing
// print(cmd, p) helpers can drive it without change. Flag-driven rendering
// options (NoColor, Verbose, …) will live on this struct.
type ReportPrinter struct {
	Report Report
}

// PrettyPrint renders the report as a human-readable string.
func (p ReportPrinter) PrettyPrint() string {
	var sb strings.Builder
	width := p.summaryColumn()
	notRun := 0
	for _, res := range p.Report.Results {
		status, reason := res.resolve()
		switch status {
		case StatusOK:
			writePassed(&sb, res, width)
		case StatusNotRun:
			notRun++
			writeNotRun(&sb, res, reason)
		default:
			writeFailed(&sb, res)
		}
	}
	writeTally(&sb, len(p.Report.Results), notRun)
	return sb.String()
}

// writePassed renders a check that found nothing wrong. If the check has a
// summary, it goes in the aligned column.
func writePassed(sb *strings.Builder, res CheckResult, width int) {
	if res.Summary == "" {
		fmt.Fprintf(sb, "✔ %s\n", res.Name)
		return
	}
	fmt.Fprintf(sb, "✔ %s%s  %s\n", res.Name, padding(res.Name, width), res.Summary)
}

// writeFailed renders the check error and every finding. It omits the summary,
// because the finding messages carry what the reader needs. A check that
// stopped part way through can also summarise only what it read.
func writeFailed(sb *strings.Builder, res CheckResult) {
	if res.Err != nil {
		fmt.Fprintf(sb, "✘ %s: check failed: %v\n", res.Name, res.Err)
	}
	for _, f := range res.Findings {
		fmt.Fprintf(sb, "✘ %s: %s\n", res.Name, f.Message)
	}
}

// writeNotRun renders a check the engine could not get a result from. The
// reason tells the reader whether a bound or a cancellation stopped it.
func writeNotRun(sb *strings.Builder, res CheckResult, reason StatusReason) {
	fmt.Fprintf(sb, "⊘ %s: did not run (%s)\n", res.Name, readableReason(reason))
}

// readableReason renders a machine-readable reason as words. Rendering the
// constant keeps the two reports from drifting apart.
func readableReason(reason StatusReason) string {
	if reason == "" {
		return "reason unknown"
	}
	return strings.ReplaceAll(string(reason), "_", " ")
}

// writeTally closes the report with the number of checks that ran. A run with
// checks that did not run says so, so a partial report cannot read as a clean
// one.
func writeTally(sb *strings.Builder, total, notRun int) {
	if notRun == 0 {
		fmt.Fprintf(sb, "ran %d checks\n", total)
		return
	}
	fmt.Fprintf(sb, "ran %d of %d checks, %d did not run\n", total-notRun, total, notRun)
}

// summaryColumn returns the column that the summaries line up in. The longest
// name that has a summary sets the width. Checks without a summary do not
// widen it.
func (p ReportPrinter) summaryColumn() int {
	width := 0
	for _, res := range p.Report.Results {
		status, _ := res.resolve()
		if res.Summary == "" || status != StatusOK {
			continue
		}
		if n := utf8.RuneCountInString(res.Name); n > width {
			width = n
		}
	}
	return width
}

// padding returns the spaces that extend name to width. It counts runes, so
// non-ASCII check names stay aligned.
func padding(name string, width int) string {
	if n := utf8.RuneCountInString(name); n < width {
		return strings.Repeat(" ", width-n)
	}
	return ""
}

// MarshalJSON delegates to the underlying Report so a printer marshals to the
// same shape as the data type it wraps.
func (p ReportPrinter) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.Report)
}
