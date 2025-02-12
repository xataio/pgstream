// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"time"

	greenmaskgenerators "github.com/eminano/greenmask/pkg/generators"
	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

func setGenerator(t greenmasktransformers.Transformer, generatorType transformers.GeneratorType) error {
	// default to using random generator
	if generatorType == "" {
		generatorType = transformers.Random
	}

	var greenmaskGenerator greenmaskgenerators.Generator
	switch generatorType {
	case transformers.Random:
		greenmaskGenerator = greenmaskgenerators.NewRandomBytes(time.Now().UnixNano(), t.GetRequiredGeneratorByteLength())
	case transformers.Deterministic:
		hashFuncName, _, err := greenmaskgenerators.GetHashFunctionNameBySize(t.GetRequiredGeneratorByteLength())
		if err != nil {
			return err
		}
		greenmaskGenerator, err = greenmaskgenerators.NewHash([]byte{}, hashFuncName)
		if err != nil {
			return err
		}
	default:
		return transformers.ErrUnsupportedGenerator
	}

	return t.SetGenerator(greenmaskGenerator)
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
