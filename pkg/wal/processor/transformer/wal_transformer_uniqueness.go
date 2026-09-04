// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"fmt"
	"sort"
	"strings"

	"github.com/xataio/pgstream/pkg/transformers"
)

type uniqueIndex struct {
	name    string
	primary bool
	columns []string
	// an expression element, e.g. lower(email), resolves to no column
	hasExpressions bool
}

func (i uniqueIndex) describe() string {
	kind := "unique index"
	if i.primary {
		kind = "primary key"
	}
	columns := i.columns
	if i.hasExpressions {
		columns = append(append([]string{}, columns...), "<expression>")
	}
	return fmt.Sprintf("%s %q (%s)", kind, i.name, strings.Join(columns, ", "))
}

// errors will collide, warnings might
type uniquenessFindings struct {
	errors   []string
	warnings []string
}

func validateUniqueness(schema, table string, indexes []uniqueIndex, columnTransformers ColumnTransformers, allowUniquenessLoss map[string]bool) uniquenessFindings {
	findings := uniquenessFindings{}
	for _, index := range indexes {
		var lossy, notGuaranteed []string
		for _, col := range index.columns {
			if allowUniquenessLoss[col] {
				continue
			}
			t, found := columnTransformers[col]
			// untransformed columns cannot collide
			if !found || t == nil {
				continue
			}
			switch transformers.UniquenessOf(t) {
			case transformers.UniquenessLossy:
				lossy = append(lossy, fmt.Sprintf("%q uses %q", col, t.Type()))
			case transformers.UniquenessNotGuaranteed:
				notGuaranteed = append(notGuaranteed, fmt.Sprintf("%q uses %q", col, t.Type()))
			}
		}

		// the expression could reference any transformed column, so the check
		// cannot clear this index either way; say so rather than stay silent
		if index.hasExpressions && len(columnTransformers) > 0 {
			findings.warnings = append(findings.warnings, fmt.Sprintf(
				"%s: %s contains expression columns that pgstream cannot analyse, so it is not checked for uniqueness; verify it by hand",
				schemaTableKey(schema, table), index.describe()))
		}

		switch {
		case len(lossy) > 0:
			findings.errors = append(findings.errors, fmt.Sprintf(
				"%s: %s is covered by a transformer that maps distinct values to the same output (%s), which will cause duplicate key violations. "+
					"Use a transformer that preserves uniqueness, such as encrypted_aes_siv or fpe_ff1, or set allow_uniqueness_loss on the column to override",
				schemaTableKey(schema, table), index.describe(), strings.Join(lossy, ", ")))
		case len(notGuaranteed) > 0:
			findings.warnings = append(findings.warnings, fmt.Sprintf(
				"%s: %s is covered by a transformer that does not guarantee unique output (%s), so duplicate key violations are possible",
				schemaTableKey(schema, table), index.describe(), strings.Join(notGuaranteed, ", ")))
		}
	}

	sort.Strings(findings.errors)
	sort.Strings(findings.warnings)
	return findings
}
