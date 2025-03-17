// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"errors"
	"fmt"
)

type Transformer interface {
	Transform(any) (any, error)
}

type Config struct {
	Name       TransformerType
	Generator  GeneratorType
	Parameters Parameters
}

type TransformerType string

const (
	String                 TransformerType = "string"
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
)

type GeneratorType string

const (
	Random        GeneratorType = "random"
	Deterministic GeneratorType = "deterministic"
)

type Parameters map[string]any

var (
	ErrUnsupportedValueType   = errors.New("unsupported value type for transformer")
	ErrUnsupportedGenerator   = errors.New("transformer doesn't support the configured generator")
	ErrUnsupportedTransformer = errors.New("unsupported transformer config")
	ErrInvalidParameters      = errors.New("invalid transformer parameters")
)

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
