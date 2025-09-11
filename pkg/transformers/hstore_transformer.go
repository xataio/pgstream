// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	greenmasktoolkit "github.com/eminano/greenmask/pkg/toolkit"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	hstoreDeleteOpName = "delete"
	hstoreSetOpName    = "set"
)

type HstoreTransformer struct {
	operations []*hstoreOperation
	hstoreVal  *hstoreValue
	buf        *bytes.Buffer
}

var (
	hstoreCompatibleTypes = []SupportedDataType{
		StringDataType,
		ByteArrayDataType,
		HstoreDataType,
	}
	hstoreParams = []Parameter{
		{
			Name:          "operations",
			SupportedType: "array",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
	}
	errKeyMustBeProvided = errors.New("key must be provided in the operation definition")
)

func NewHstoreTransformer(params ParameterValues) (*HstoreTransformer, error) {
	operations, err := getHstoreOperations(params)
	if err != nil {
		return nil, fmt.Errorf("hstore_transformer: error finding operations parameter: %w", err)
	}

	// prepare the template objects for operations that has template values
	for idx, o := range operations {
		if o.valueTemplate != "" {
			tmpl, err := template.New(fmt.Sprintf("op[%d] %s %s", idx, o.operation, o.key)).
				Funcs(greenmasktoolkit.FuncMap()).
				Funcs(sprig.FuncMap()).
				Parse(o.valueTemplate)
			if err != nil {
				return nil, fmt.Errorf("hstore_transformer: error parsing template op[%d] with key \"%s\": %w", idx, o.key, err)
			}
			o.tmpl = tmpl
		}
	}

	return &HstoreTransformer{
		operations: operations,
		buf:        bytes.NewBuffer(nil),
		hstoreVal:  &hstoreValue{},
	}, nil
}

func (t *HstoreTransformer) Transform(_ context.Context, value Value) (any, error) {
	var err error
	var toTransform pgtype.Hstore
	switch val := value.TransformValue.(type) {
	case []byte:
		err = toTransform.Scan(string(val))
	case string:
		err = toTransform.Scan(val)
	case pgtype.Hstore:
		toTransform = val
	default:
		return nil, ErrUnsupportedValueType
	}

	if err != nil {
		return nil, fmt.Errorf("hstore_transformer: error parsing hstore: %w", err)
	}

	// set dynamic values for the hstoreValue instance, to be used in templates
	t.hstoreVal.setDynamicValues(value.DynamicValues)

	transformed := toTransform
	for idx, op := range t.operations {
		t.hstoreVal.setValue(transformed, op.key)
		if op.skipNotExist && !t.hstoreVal.exists {
			continue
		}
		// apply each operation in the order they were provided
		transformed, err = op.apply(transformed, t.hstoreVal, t.buf)
		if err != nil {
			return nil, fmt.Errorf("hstore_transformer: cannot apply \"%s\" operation[%d] with key %s: %w", op.operation, idx, op.key, err)
		}
	}

	// if hstore type is requested, return right away
	if _, ok := value.TransformValue.(pgtype.Hstore); ok {
		return transformed, nil
	}

	// for string and []byte, we need to convert the hstore back to the original format
	hstoreValue, err := transformed.Value()
	if err != nil {
		return nil, fmt.Errorf("hstore_transformer: error encoding hstore: %w", err)
	}

	hstoreStr, _ := hstoreValue.(string)
	switch value.TransformValue.(type) {
	case string:
		return hstoreStr, nil
	case []byte:
		return []byte(hstoreStr), nil
	default:
		// not expected, since already checked at the beginning of Transform
		return nil, ErrUnsupportedValueType
	}
}

func (t *HstoreTransformer) CompatibleTypes() []SupportedDataType {
	return hstoreCompatibleTypes
}

func (t *HstoreTransformer) Type() TransformerType {
	return Hstore
}

func HstoreTransformerDefinition() *Definition {
	return &Definition{
		SupportedTypes: hstoreCompatibleTypes,
		Parameters:     hstoreParams,
	}
}

func getHstoreOperations(params ParameterValues) ([]*hstoreOperation, error) {
	arrayAny, found, err := FindParameter[[]any](params, "operations")
	if err != nil {
		return nil, fmt.Errorf("operations must be an array: %w", err)
	}
	if !found || len(arrayAny) == 0 {
		return nil, errOperationsMustBeProvided
	}

	operations := make([]*hstoreOperation, 0, len(arrayAny))
	for _, valAny := range arrayAny {
		val, ok := valAny.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid element type in operations array, got %T: %w", valAny, ErrInvalidParameters)
		}

		op := &hstoreOperation{}
		op.operation, found, err = FindParameter[string](val, "operation")
		if err != nil {
			return nil, fmt.Errorf("operation name must be a string: %w", err)
		}
		if !found {
			return nil, errOperationNameMustBeProvided
		}
		if op.operation != hstoreSetOpName && op.operation != hstoreDeleteOpName {
			return nil, errInvalidOperationsType
		}

		op.key, found, err = FindParameter[string](val, "key")
		if err != nil {
			return nil, fmt.Errorf("key must be a string: %w", err)
		}
		if !found {
			return nil, errKeyMustBeProvided
		}

		op.errorNotExist, err = FindParameterWithDefault(val, "error_not_exist", false)
		if err != nil {
			return nil, fmt.Errorf("error_not_exist must be a boolean: %w", err)
		}

		op.skipNotExist, err = FindParameterWithDefault(val, "skip_not_exist", false)
		if err != nil {
			return nil, fmt.Errorf("skip_not_exist must be a boolean: %w", err)
		}

		if op.operation == hstoreDeleteOpName {
			operations = append(operations, op)
			continue
		}

		// get value for set operation
		valueNullable, valueFound := val["value"]
		if valueFound {
			if valueNullable == nil {
				op.value = nil
			} else {
				strVal, ok := valueNullable.(string)
				if !ok {
					return nil, fmt.Errorf("cannot read parameter 'value', got %T: %w", valueNullable, ErrInvalidParameters)
				}
				op.value = &strVal
			}
		}

		// get value template for set operation
		var valueTemplateFound bool
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
