// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"encoding/json"
	"fmt"
	"strings"
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
	for _, res := range p.Report.Results {
		if res.Err == nil && len(res.Findings) == 0 {
			fmt.Fprintf(&sb, "✔ %s\n", res.Name)
			continue
		}
		if res.Err != nil {
			fmt.Fprintf(&sb, "✘ %s: check failed: %v\n", res.Name, res.Err)
		}
		for _, f := range res.Findings {
			fmt.Fprintf(&sb, "✘ %s: %s\n", res.Name, f.Message)
		}
	}
	fmt.Fprintf(&sb, "ran %d checks\n", len(p.Report.Results))
	return sb.String()
}

// MarshalJSON delegates to the underlying Report so a printer marshals to the
// same shape as the data type it wraps.
func (p ReportPrinter) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.Report)
}
