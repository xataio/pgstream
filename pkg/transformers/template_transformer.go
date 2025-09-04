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
	"github.com/jackc/pgx/v5/pgtype"
)

type TemplateTransformer struct {
	template *template.Template
}

var (
	errTemplateMustBeProvided = errors.New("template_transformer: template parameter must be provided")
	templateCompatibleTypes   = []SupportedDataType{
		StringDataType,
		ByteArrayDataType,
		HstoreDataType,
	}
	templateParams = []Parameter{
		{
			Name:          "template",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
	}
)

func NewTemplateTransformer(params ParameterValues) (*TemplateTransformer, error) {
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
	if _, ok := value.TransformValue.(pgtype.Hstore); ok {
		return t.transformHstore(value)
	}

	var buf strings.Builder
	if err := t.template.Execute(&buf, &value); err != nil {
		return nil, fmt.Errorf("template_transformer: error executing template: %w", err)
	}
	return buf.String(), nil
}

func (t *TemplateTransformer) CompatibleTypes() []SupportedDataType {
	return templateCompatibleTypes
}

func (t *TemplateTransformer) Type() TransformerType {
	return Template
}

func TemplateTransformerDefinition() *Definition {
	return &Definition{
		SupportedTypes: templateCompatibleTypes,
		Parameters:     templateParams,
	}
}

func (t *TemplateTransformer) transformHstore(value Value) (pgtype.Hstore, error) {
	hstoreValue, _ := value.TransformValue.(pgtype.Hstore)
	strValue, err := hstoreValue.Value()
	if err != nil {
		return nil, fmt.Errorf("template_transformer: error getting hstore value: %w", err)
	}
	value.TransformValue = strValue
	var buf strings.Builder
	if err := t.template.Execute(&buf, &value); err != nil {
		return nil, fmt.Errorf("template_transformer: error executing template: %w", err)
	}
	transformedHstore, err := parseHstore(strings.TrimSpace(buf.String()))
	if err != nil {
		return nil, fmt.Errorf("template_transformer: error parsing hstore: %w", err)
	}
	return transformedHstore, nil
}
