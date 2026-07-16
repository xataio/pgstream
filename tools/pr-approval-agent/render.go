// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"strings"
)

// GitHub comment rendering for the agent's verdicts.

func renderApproval(c classification, v verdict) string {
	return fmt.Sprintf(`%s
✅ **Approved by the pgstream review agent.**

> %s

<sub>tier `+"`%s`"+` · %d files / %d substantive lines · risk `+"`%s`"+`. This is an automated review and does not replace human judgement on anything it escalates.</sub>`,
		stickyMarker, v.Reasoning, c.Tier, c.SubstantiveFiles, c.SubstantiveLines, v.Risk)
}

func renderNonApproval(c classification, gate gateResult, v *verdict, final, label string) string {
	icon := "⚠️"
	header := "The pgstream review agent is deferring to a human reviewer"
	if final == "REFUSE" {
		icon = "🛑"
		header = "The pgstream review agent found a problem"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s\n%s **%s.**\n\n", stickyMarker, icon, header)
	if v != nil {
		fmt.Fprintf(&b, "> %s\n\n", v.Reasoning)
		for _, issue := range v.Issues {
			fmt.Fprintf(&b, "- %s\n", issue)
		}
	}
	for _, reason := range gate.Reasons {
		fmt.Fprintf(&b, "- %s\n", reason)
	}
	fmt.Fprintf(&b, "\n<sub>tier `%s`. Address the above (or get a human review) and re-add the `%s` label to re-run.</sub>", c.Tier, label)
	return b.String()
}
