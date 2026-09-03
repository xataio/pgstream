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
	for _, res := range p.Report.Results {
		if res.Err == nil && len(res.Findings) == 0 {
			writePassed(&sb, res, width)
			continue
		}
		writeFailed(&sb, res)
	}
	fmt.Fprintf(&sb, "ran %d checks\n", len(p.Report.Results))
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

// summaryColumn returns the column that the summaries line up in. The longest
// name that has a summary sets the width. Checks without a summary do not
// widen it.
func (p ReportPrinter) summaryColumn() int {
	width := 0
	for _, res := range p.Report.Results {
		if res.Summary == "" || res.Err != nil || len(res.Findings) > 0 {
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
