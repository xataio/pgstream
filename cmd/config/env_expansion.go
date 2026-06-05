// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"regexp"
)

var bracedEnvVarPattern = regexp.MustCompile(`\$\{[A-Za-z_][A-Za-z0-9_]*\}`)

func expandBracedEnvVars(input string) string {
	return bracedEnvVarPattern.ReplaceAllStringFunc(input, func(match string) string {
		return os.Getenv(match[2 : len(match)-1])
	})
}
