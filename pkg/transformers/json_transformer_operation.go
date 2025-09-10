// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"bytes"
	"fmt"
	"slices"
	"text/template"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var (
	jsonSetOpt = &sjson.Options{
		ReplaceInPlace: true,
	}
	errDynamicValuesNil = fmt.Errorf("dynamic values are nil")
)

type jsonOperation struct {
	operation     string
	value         any
	valueTemplate string
	path          string
	errorNotExist bool
	tmpl          *template.Template
}

func (o *jsonOperation) apply(inp []byte, jsonVal *jsonValue, buf *bytes.Buffer) ([]byte, error) {
	var res []byte
	var err error

	switch o.operation {
	case jsonSetOpName:
		res = slices.Clone(inp)
		if o.tmpl != nil {
			buf.Reset()
			jsonVal.setValue(inp, o.path)
			if o.errorNotExist && !jsonVal.exists {
				return nil, fmt.Errorf("value by path \"%s\" does not exist", o.path)
			}

			if jsonVal.value != interface{}(nil) {
				if err = o.tmpl.Execute(buf, jsonVal); err != nil {
					return nil, fmt.Errorf("error executing template: %w", err)
				}
				newValue := buf.Bytes()

				res, err = sjson.SetRawBytesOptions(inp, o.path, newValue, jsonSetOpt)
				if err != nil {
					return nil, fmt.Errorf("error applying set raw operation: %w", err)
				}
			}
		} else {
			res, err = sjson.SetBytesOptions(inp, o.path, o.value, jsonSetOpt)
			if err != nil {
				return nil, fmt.Errorf("error applying set operation: %w", err)
			}
		}

	case jsonDeleteOpName:
		if o.errorNotExist && !gjson.GetBytes(inp, o.path).Exists() {
			return nil, fmt.Errorf("value by path \"%s\" does not exist", o.path)
		}
		res, err = sjson.DeleteBytes(inp, o.path)
		if err != nil {
			return nil, fmt.Errorf("error applying delete operation: %w", err)
		}

	default:
		return nil, fmt.Errorf("unknown operation %s", o.operation)
	}

	return res, nil
}

type jsonValue struct {
	exists        bool
	value         any
	dynamicValues map[string]any
}

func (jv *jsonValue) setValue(data []byte, path string) {
	res := gjson.GetBytes(data, path)
	jv.value = res.Value()
	jv.exists = res.Exists()
}

func (jv *jsonValue) setDynamicValues(values map[string]any) {
	jv.dynamicValues = values
}

func (jv *jsonValue) GetValue() any {
	if !jv.exists {
		return nil
	}
	return jv.value
}

func (jv *jsonValue) GetDynamicValue(key string) (any, error) {
	if jv.dynamicValues == nil {
		return nil, errDynamicValuesNil
	}
	dynValue, found := jv.dynamicValues[key]
	if !found {
		return nil, fmt.Errorf("dynamic value '%s' not found", key)
	}
	return dynValue, nil
}
