// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	greenmasktoolkit "github.com/eminano/greenmask/pkg/toolkit"
	"github.com/xataio/pgstream/internal/json"
)

const (
	jsonDeleteOpName = "delete"
	jsonSetOpName    = "set"
)

var (
	errOperationsMustBeProvided      = errors.New("operations parameter must be provided")
	errOperationsCannotBeEmpty       = errors.New("operations cannot be empty")
	errInvalidOperationsType         = errors.New("unknown operation name, must be one of 'set' or 'delete'")
	errOperationNameMustBeProvided   = errors.New("operation name must be provided in the operation definition")
	errValueOrTemplateMustBeProvided = errors.New("either value or value_template must be provided in the 'set' operation definition")
	errPathMustBeProvided            = errors.New("path must be provided in the operation definition")
	jsonCompatibleTypes              = []SupportedDataType{
		JSONDataType,
	}
	jsonParams = []Parameter{
		{
			Name:          "operations",
			SupportedType: "array",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
	}
)

type JSONTransformer struct {
	operations []*jsonOperation
	jsonVal    *jsonValue
	buf        *bytes.Buffer
}

func NewJSONTransformer(params ParameterValues) (*JSONTransformer, error) {
	operations, err := getOperationsParam(params)
	if err != nil {
		return nil, fmt.Errorf("json_transformer: error finding operations parameter: %w", err)
	}

	// prepare the template objects for operations that has template values
	for idx, o := range operations {
		if o.valueTemplate != "" {
			tmpl, err := template.New(fmt.Sprintf("op[%d] %s %s", idx, o.operation, o.path)).
				Funcs(greenmasktoolkit.FuncMap()).
				Funcs(sprig.FuncMap()).
				Parse(o.valueTemplate)
			if err != nil {
				return nil, fmt.Errorf("json_transformer: error parsing template op[%d] with path \"%s\": %w", idx, o.path, err)
			}
			o.tmpl = tmpl
		}
	}

	return &JSONTransformer{
		operations: operations,
		buf:        bytes.NewBuffer(nil),
		jsonVal:    &jsonValue{},
	}, nil
}

func (jt *JSONTransformer) Transform(_ context.Context, value Value) (any, error) {
	var err error
	var toTransform []byte
	switch val := value.TransformValue.(type) {
	case []byte:
		toTransform = val
	case string:
		toTransform = []byte(val)
	default:
		toTransform, err = json.Marshal(value.TransformValue)
		if err != nil {
			return nil, fmt.Errorf("json_transformer: error marshalling value to JSON: %w", err)
		}
	}
	// set dynamic values for the jsonValue instance, to be used in templates
	jt.jsonVal.setDynamicValues(value.DynamicValues)

	res := slices.Clone(toTransform)
	for idx, op := range jt.operations {
		// apply each operation in the order they were provided
		res, err = op.apply(res, jt.jsonVal, jt.buf)
		if err != nil {
			return nil, fmt.Errorf("cannot apply \"%s\" operation[%d] with path %s: %w", op.operation, idx, op.path, err)
		}
	}

	var resJSON any
	if err = json.Unmarshal(res, &resJSON); err != nil {
		return nil, fmt.Errorf("json_transformer: error unmarshalling the result: %w; JSON: %s", err, string(res))
	}
	return resJSON, nil
}

func (jt *JSONTransformer) CompatibleTypes() []SupportedDataType {
	return jsonCompatibleTypes
}

func (jt *JSONTransformer) Type() TransformerType {
	return JSON
}

func JSONTransformerDefinition() *Definition {
	return &Definition{
		SupportedTypes: jsonCompatibleTypes,
		Parameters:     jsonParams,
	}
}

func getOperationsParam(params ParameterValues) ([]*jsonOperation, error) {
	arrayAny, found, err := FindParameter[[]any](params, "operations")
	if err != nil {
		return nil, fmt.Errorf("operations must be an array: %w", err)
	}
	if !found {
		return nil, errOperationsMustBeProvided
	}
	if len(arrayAny) == 0 {
		return nil, errOperationsCannotBeEmpty
	}

	operations := make([]*jsonOperation, 0, len(arrayAny))
	for _, valAny := range arrayAny {
		val, ok := valAny.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid element type in operations array, got %T: %w", valAny, ErrInvalidParameters)
		}

		op := &jsonOperation{}
		op.operation, found, err = FindParameter[string](val, "operation")
		if err != nil {
			return nil, fmt.Errorf("operation name must be a string: %w", err)
		}
		if !found {
			return nil, errOperationNameMustBeProvided
		}
		if op.operation != jsonSetOpName && op.operation != jsonDeleteOpName {
			return nil, errInvalidOperationsType
		}

		op.path, found, err = FindParameter[string](val, "path")
		if err != nil {
			return nil, fmt.Errorf("path must be a string: %w", err)
		}
		if !found {
			return nil, errPathMustBeProvided
		}

		op.errorNotExist, err = FindParameterWithDefault(val, "error_not_exist", false)
		if err != nil {
			return nil, fmt.Errorf("error_not_exist must be a boolean: %w", err)
		}

		if op.operation == jsonDeleteOpName {
			operations = append(operations, op)
			continue
		}

		// get value for set operation
		var valueTemplateFound, valueFound bool
		op.value, valueFound, err = FindParameter[any](val, "value")
		if err != nil {
			return nil, fmt.Errorf("cannot read parameter 'value': %w", err)
		}

		// get value template for set operation
		op.valueTemplate, valueTemplateFound, err = FindParameter[string](val, "value_template")
		if err != nil {
			return nil, fmt.Errorf("value_template must be a string: %w", err)
		}

		// ensure that at least one of value or value_template is provided
		if !valueFound && !valueTemplateFound {
			return nil, errValueOrTemplateMustBeProvided
		}

		operations = append(operations, op)
	}

	return operations, nil
}
