// SPDX-License-Identifier: Apache-2.0

package transformer

type Config struct {
	InferFromSecurityLabels bool
	DumpInferredRules       bool
	TransformerRules        []TableRules
	ValidationMode          string
}

func (c *Config) HasNoRules() bool {
	return c == nil || len(c.TransformerRules) == 0
}

func (c *Config) validationMode() string {
	if c.ValidationMode == "" {
		return validationModeRelaxed
	}
	return c.ValidationMode
}
