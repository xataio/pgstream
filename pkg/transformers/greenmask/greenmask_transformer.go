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

func setGenerator(t greenmasktransformers.Transformer, params transformers.Parameters) error {
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

func getGeneratorType(params transformers.Parameters) (string, error) {
	// default to using the random generator
	return findParameter(params, "generator", random)
}

func findParameter[T any](params transformers.Parameters, name string, defaultVal T) (T, error) {
	var found bool
	var err error

	var val T
	val, found, err = transformers.FindParameter[T](params, name)
	if err != nil {
		return val, err
	}
	if !found {
		return defaultVal, nil
	}
	return val, nil
}

func findParameterArray[T any](params transformers.Parameters, name string, defaultVal []T) ([]T, error) {
	val, found, err := transformers.FindParameterArray[T](params, name)
	if err != nil {
		return val, err
	}
	if !found {
		return defaultVal, nil
	}
	return val, nil
}
