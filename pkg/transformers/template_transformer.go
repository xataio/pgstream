// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	greenmasktoolkit "github.com/eminano/greenmask/pkg/toolkit"
)

type TemplateTransformer struct {
	template *template.Template
}

var (
	errTemplateMustBeProvided = errors.New("template_transformer: template parameter must be provided")
	TemplateCompatibleTypes   = []SupportedDataType{
		StringDataType,
		ByteArrayDataType,
	}
	TemplateParams = []TransformerParameter{
		{
			Name:          "template",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
	}
)

func NewTemplateTransformer(params Parameters) (*TemplateTransformer, error) {
	templateStr, found, err := FindParameter[string](params, "template")
	if err != nil {
		return nil, fmt.Errorf("template_transformer: template must be a string: %w", err)
	}
	if !found {
		return nil, errTemplateMustBeProvided
	}

	tmpl, err := template.New("").Funcs(greenmasktoolkit.FuncMap()).Funcs(sprig.FuncMap()).Parse(templateStr)
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
	return TemplateCompatibleTypes
}

func (t *TemplateTransformer) Type() TransformerType {
	return Template
}
