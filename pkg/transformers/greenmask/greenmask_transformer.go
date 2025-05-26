// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"time"

	greenmaskgenerators "github.com/eminano/greenmask/pkg/generators"
	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

const (
	random        = "random"
	deterministic = "deterministic"
)

func setGenerator(t greenmasktransformers.Transformer, params transformers.ParameterValues) error {
	generatorType, err := getGeneratorType(params)
	if err != nil {
		return err
	}

	var greenmaskGenerator greenmaskgenerators.Generator
	switch generatorType {
	case random:
		greenmaskGenerator = greenmaskgenerators.NewRandomBytes(time.Now().UnixNano(), t.GetRequiredGeneratorByteLength())
	case deterministic:
		var err error
		greenmaskGenerator, err = greenmaskgenerators.GetHashBytesGen([]byte{}, t.GetRequiredGeneratorByteLength())
		if err != nil {
			return err
		}
	default:
		return transformers.ErrUnsupportedGenerator
	}

	return t.SetGenerator(greenmaskGenerator)
}

func getGeneratorType(params transformers.ParameterValues) (string, error) {
	// default to using the random generator
	return findParameter(params, "generator", random)
}

func findParameter[T any](params transformers.ParameterValues, name string, defaultVal T) (T, error) {
	return transformers.FindParameterWithDefault(params, name, defaultVal)
}

func findParameterArray[T any](params transformers.ParameterValues, name string, defaultVal []T) ([]T, error) {
	val, found, err := transformers.FindParameterArray[T](params, name)
	if err != nil {
		return val, err
	}
	if !found {
		return defaultVal, nil
	}
	return val, nil
}
