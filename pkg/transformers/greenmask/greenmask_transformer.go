// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"time"

	greenmaskgenerators "github.com/eminano/greenmask/pkg/generators"
	"github.com/xataio/pgstream/pkg/transformers"
)

func getGreenmaskGenerator(requiredByteLength int, generatorType transformers.GeneratorType) (greenmaskgenerators.Generator, error) {
	// default to using random generator
	if generatorType == "" || generatorType == transformers.Random {
		generatorType = transformers.Random
	}

	var greenmaskGenerator greenmaskgenerators.Generator
	switch generatorType {
	case transformers.Random:
		greenmaskGenerator = greenmaskgenerators.NewRandomBytes(time.Now().UnixNano(), requiredByteLength)
	case transformers.Deterministic:
		hashFuncName, _, err := greenmaskgenerators.GetHashFunctionNameBySize(requiredByteLength)
		if err != nil {
			return nil, err
		}
		greenmaskGenerator, err = greenmaskgenerators.NewHash([]byte{}, hashFuncName)
		if err != nil {
			return nil, err
		}
	default:
		return nil, transformers.ErrUnsupportedGenerator
	}

	return greenmaskGenerator, nil
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
