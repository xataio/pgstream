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
	template             *template.Template
	templateStr          string
	RequiredTransformers map[string]Config
}

var (
	errTemplateMustBeProvided          = errors.New("template_transformer: template parameter must be provided")
	errExposedNameMustBeProvided       = errors.New("template_transformer: template_function_name must be provided in the functions array elements")
	errTransformerNameMustBeProvided   = errors.New("template_transformer: name must be provided in the functions array elements")
	errTransformerParamsMustBeProvided = errors.New("template_transformer: parameters must be provided in the functions array elements")
	templateCompatibleTypes            = []SupportedDataType{
		StringDataType,
		ByteArrayDataType,
	}
	templateParams = []Parameter{
		{
			Name:          "template",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
		{
			Name:          "functions",
			SupportedType: "array",
			Default:       nil,
			Dynamic:       false,
			Required:      false,
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

	requiredTransformers, err := getRequiredTransformersConfig(params)
	if err != nil {
		return nil, fmt.Errorf("template_transformer: error getting required transformers config: %w", err)
	}

	return &TemplateTransformer{templateStr: templateStr, RequiredTransformers: requiredTransformers}, nil
}

func (t *TemplateTransformer) PostCreate(param any) error {
	funcMap, ok := param.(map[string]any)
	if !ok {
		return fmt.Errorf("template_transformer: expected map[string]any for PostCreate parameter, got %T", param)
	}

	tmpl, err := template.New("").Funcs(greenmasktoolkit.FuncMap()).Funcs(funcMap).Parse(t.templateStr)
	if err != nil {
		return fmt.Errorf("template_transformer: error parsing template: %w", err)
	}

	t.template = tmpl
	return nil
}

func (t *TemplateTransformer) Transform(_ context.Context, value Value) (any, error) {
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

func (t *TemplateTransformer) Close() error {
	return nil
}

func TemplateTransformerDefinition() *Definition {
	return &Definition{
		SupportedTypes: templateCompatibleTypes,
		Parameters:     templateParams,
	}
}

func getRequiredTransformersConfig(params ParameterValues) (map[string]Config, error) {
	arrayAny, _, err := FindParameter[[]any](params, "functions")
	if err != nil {
		return nil, fmt.Errorf("functions must be an array: %w", err)
	}
	if len(arrayAny) == 0 {
		return nil, nil
	}

	requiredTransformersConfig := make(map[string]Config, len(arrayAny))
	for _, valAny := range arrayAny {
		val, ok := valAny.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid element type in functions array, got %T: %w", valAny, ErrInvalidParameters)
		}

		exposedName, found, err := FindParameter[string](val, "template_function_name")
		if err != nil {
			return nil, fmt.Errorf("template_function_name must be a string: %w", err)
		}
		if !found {
			return nil, errExposedNameMustBeProvided
		}
		if _, found := requiredTransformersConfig[exposedName]; found {
			return nil, fmt.Errorf("duplicate template_function_name found: %s", exposedName)
		}

		transformerName, found, err := FindParameter[string](val, "name")
		if err != nil {
			return nil, fmt.Errorf("name must be a string: %w", err)
		}
		if !found {
			return nil, errTransformerNameMustBeProvided
		}

		transformerParams, found, err := FindParameter[map[string]any](val, "parameters")
		if err != nil {
			return nil, fmt.Errorf("parameters must be a map: %w", err)
		}
		if !found {
			return nil, errTransformerParamsMustBeProvided
		}

		requiredTransformersConfig[exposedName] = Config{
			Name:       TransformerType(transformerName),
			Parameters: transformerParams,
		}
	}
	return requiredTransformersConfig, nil
}
