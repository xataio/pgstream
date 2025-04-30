// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"errors"
	"fmt"
)

type Transformer interface {
	Transform(Value) (any, error)
	CompatibleTypes() []SupportedDataType
}

type Value struct {
	TransformValue any
	DynamicValues  map[string]any
}

type DynamicParameter struct {
	Column string
}

type Config struct {
	Name              TransformerType
	Parameters        Parameters
	DynamicParameters Parameters
}

type TransformerType string

const (
	String                 TransformerType = "string"
	LiteralString          TransformerType = "literal_string"
	PhoneNumber            TransformerType = "phone_number"
	NeosyncString          TransformerType = "neosync_string"
	GreenmaskString        TransformerType = "greenmask_string"
	NeosyncFirstName       TransformerType = "neosync_firstname"
	GreenmaskFirstName     TransformerType = "greenmask_firstname"
	NeosyncEmail           TransformerType = "neosync_email"
	GreenmaskInteger       TransformerType = "greenmask_integer"
	GreenmaskFloat         TransformerType = "greenmask_float"
	GreenmaskUUID          TransformerType = "greenmask_uuid"
	GreenmaskBoolean       TransformerType = "greenmask_boolean"
	GreenmaskChoice        TransformerType = "greenmask_choice"
	GreenmaskUnixTimestamp TransformerType = "greenmask_unix_timestamp"
	GreenmaskDate          TransformerType = "greenmask_date"
	GreenmaskUTCTimestamp  TransformerType = "greenmask_utc_timestamp"
	Masking                TransformerType = "masking"
)

type SupportedDataType string

const (
	ByteArrayDataType      SupportedDataType = "byte_array"
	StringDataType         SupportedDataType = "string"
	BooleanDataType        SupportedDataType = "boolean"
	Integer8DataType       SupportedDataType = "integer8"
	UInteger8DataType      SupportedDataType = "uinteger8"
	Integer16DataType      SupportedDataType = "integer16"
	UInteger16DataType     SupportedDataType = "uinteger16"
	Integer32DataType      SupportedDataType = "integer32"
	UInteger32DataType     SupportedDataType = "uinteger32"
	Integer64DataType      SupportedDataType = "integer64"
	UInteger64DataType     SupportedDataType = "uinteger64"
	Float32DataType        SupportedDataType = "float32"
	Float64DataType        SupportedDataType = "float64"
	UUIDDataType           SupportedDataType = "uuid"
	UInt8ArrayOf16DataType SupportedDataType = "uint8_array_of_16"
	DateDataType           SupportedDataType = "date"
	DatetimeDataType       SupportedDataType = "datetime"
	JSONDataType           SupportedDataType = "json"
)

const (
	columnDynamicParam = "column"
)

type Parameters map[string]any

var (
	ErrUnsupportedValueType     = errors.New("unsupported value type for transformer")
	ErrUnsupportedGenerator     = errors.New("transformer doesn't support the configured generator")
	ErrUnsupportedTransformer   = errors.New("unsupported transformer config")
	ErrInvalidParameters        = errors.New("invalid transformer parameters")
	ErrInvalidDynamicParameters = errors.New("invalid transformer dynamic parameters")
	ErrUnknownParameter         = errors.New("unknown parameter provided to transformer")
)

func NewValue(transformValue any, dynamicValues map[string]any) Value {
	return Value{
		TransformValue: transformValue,
		DynamicValues:  dynamicValues,
	}
}

func FindParameter[T any](params Parameters, name string) (T, bool, error) {
	valAny, found := params[name]
	if !found {
		return *new(T), false, nil
	}

	val, ok := valAny.(T)
	if !ok {
		return *new(T), true, fmt.Errorf("got %T: %w", valAny, ErrInvalidParameters)
	}

	return val, true, nil
}

func FindParameterWithDefault[T any](params Parameters, name string, defaultValue T) (T, error) {
	val, found, err := FindParameter[T](params, name)
	if err != nil {
		return val, err
	}
	if !found {
		return defaultValue, nil
	}
	return val, nil
}

func FindParameterArray[T any](params Parameters, name string) ([]T, bool, error) {
	// first check if the array is of the expected type
	arrValue, found, err := FindParameter[[]T](params, name)
	if err == nil {
		return arrValue, found, nil
	}

	// check if the array is of interface type instead of the expected type
	arrayAny, ok := params[name].([]any)
	if !ok {
		return nil, true, ErrInvalidParameters
	}

	valArray := make([]T, 0, len(arrayAny))
	for _, valAny := range arrayAny {
		val, ok := valAny.(T)
		if !ok {
			return nil, true, fmt.Errorf("array: got %T: %w", valAny, ErrInvalidParameters)
		}
		valArray = append(valArray, val)
	}

	return valArray, true, nil
}

func ParseDynamicParameters(params Parameters) (map[string]*DynamicParameter, error) {
	dynamicParamMap := make(map[string]*DynamicParameter, len(params))
	for param, anyVal := range params {
		if param == "" {
			return nil, ErrInvalidDynamicParameters
		}

		dynamicParam, ok := anyVal.(map[string]any)
		if !ok {
			return nil, ErrInvalidDynamicParameters
		}

		column, found, err := FindParameter[string](dynamicParam, columnDynamicParam)
		if err != nil {
			return nil, fmt.Errorf("dynamic parameter column must be of type string: %w", err)
		}
		if !found {
			return nil, fmt.Errorf("dynamic parameter must have column field: %w", ErrInvalidDynamicParameters)
		}

		dynamicParamMap[param] = &DynamicParameter{
			Column: column,
		}
	}

	return dynamicParamMap, nil
}

func FindDynamicValue[T any](param *DynamicParameter, dynamicValues map[string]any, defaultValue T) (T, error) {
	dynValue, found, err := FindParameter[T](dynamicValues, param.Column)
	if err != nil {
		return dynValue, err
	}
	if !found {
		return defaultValue, nil
	}

	return dynValue, nil
}

// ValidateParameters checks if all provided parameters are in the expected set
func ValidateParameters(provided map[string]any, expected []string) error {
	expectedMap := make(map[string]struct{}, len(expected))
	for _, param := range expected {
		expectedMap[param] = struct{}{}
	}

	for key := range provided {
		if _, ok := expectedMap[key]; !ok {
			return fmt.Errorf("%w: unexpected parameter '%s'", ErrUnknownParameter, key)
		}
	}

	return nil
}
