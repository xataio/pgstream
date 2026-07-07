// SPDX-License-Identifier: Apache-2.0

package transformer

type Config struct {
	InferFromSecurityLabels bool
	DumpInferredRules       bool
	TransformerRules        []TableRules
	ValidationMode          string
	OnError                 string
}

func (c *Config) HasNoRules() bool {
	return c == nil || len(c.TransformerRules) == 0
}

func (c *Config) onError() string {
	if c.OnError == "" {
		return OnErrorNull
	}
	return c.OnError
}

func (c *Config) validationMode() string {
	if c.ValidationMode == "" {
		return validationModeRelaxed
	}
	return c.ValidationMode
}
