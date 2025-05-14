// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"text/template"

	greenmasktoolkit "github.com/eminano/greenmask/pkg/toolkit"
)

type TemplateTransformer struct {
	template *template.Template
}

var (
	templateTransformerParams = []string{"template"}
	errTemplateMustBeProvided = errors.New("template_transformer: template parameter must be provided")
)

func NewTemplateTransformer(params Parameters) (*TemplateTransformer, error) {
	if err := ValidateParameters(params, templateTransformerParams); err != nil {
		return nil, err
	}
	templateStr, found, err := FindParameter[string](params, "template")
	if err != nil {
		return nil, fmt.Errorf("template_transformer: template must be a string: %w", err)
	}
	if !found {
		return nil, errTemplateMustBeProvided
	}

	tmpl, err := template.New("").Funcs(greenmasktoolkit.FuncMap()).Parse(templateStr)
	if err != nil {
		return nil, fmt.Errorf("template_transformer: error parsing template: %w", err)
	}
	return &TemplateTransformer{template: tmpl}, nil
}

func (t *TemplateTransformer) Transform(_ context.Context, value Value) (any, error) {
	var buf strings.Builder
	if err := t.template.Execute(&buf, &value); err != nil {
		return nil, fmt.Errorf("template_transformer: error executing template: %w", err)
	}
	return buf.String(), nil
}

func (t *TemplateTransformer) CompatibleTypes() []SupportedDataType {
	return []SupportedDataType{
		AllDataTypes,
	}
}

func (t *TemplateTransformer) Type() TransformerType {
	return Template
}
