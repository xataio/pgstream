// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
	"github.com/xataio/pgstream/pkg/transformers/generators"
)

const (
	random        = "random"
	deterministic = "deterministic"
)

// setGenerator configures the transformer with a generator that is safe for
// concurrent use. pgstream shares one transformer per column across all
// snapshot worker goroutines, and the generator is the only mutable state in
// the greenmask transformers (except for the string one, which pools whole
// instances), so this makes the transformers safe to call concurrently
// (issues #789 and #800).
func setGenerator(t greenmasktransformers.Transformer, params transformers.ParameterValues) error {
	generatorType, err := getGeneratorType(params)
	if err != nil {
		return err
	}

	var generator generators.Generator
	switch generatorType {
	case random:
		generator = generators.NewRandomBytesGenerator(t.GetRequiredGeneratorByteLength())
	case deterministic:
		generator, err = generators.NewDeterministicBytesGenerator(t.GetRequiredGeneratorByteLength())
		if err != nil {
			return err
		}
	default:
		return transformers.ErrUnsupportedGenerator
	}

	return t.SetGenerator(generator)
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
