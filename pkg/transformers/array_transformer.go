// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/jackc/pgx/v5/pgtype"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/transformers/internal/pool"
)

// maxArrayElementCount bounds the output length of the random generator. It
// guards against a mistyped max_count, which would otherwise allocate and
// transform that many elements for every row, once per element for a
// transformer that queries the source.
const maxArrayElementCount = 10_000

// ArrayGenerator selects how the elements of the transformed array are drawn
// from the elements of the source array.
type ArrayGenerator string

const (
	// ArrayGeneratorMap transforms every source element in order, preserving
	// the length of the array.
	ArrayGeneratorMap ArrayGenerator = "map"
	// ArrayGeneratorRandom emits between MinCount and MaxCount elements, each
	// the transform of a source element chosen at random with replacement.
	ArrayGeneratorRandom ArrayGenerator = "random"
)

// arrayShape records how the value reached the transformer, so that the
// transformed value is emitted the same way. The replication path carries an
// array as its postgres literal, the snapshot path as a decoded slice, and
// the kafka, webhook and search targets serialise whichever they are given.
type arrayShape int

const (
	shapeSlice arrayShape = iota
	shapeString
	shapeBytes
)

var (
	ErrMultiDimensionalArray = errors.New("multi-dimensional arrays are not supported")
	ErrInvalidArrayOptions   = errors.New("invalid array options")
)

// ArrayConfig describes an ArrayTransformer.
type ArrayConfig struct {
	ElementTransformer Transformer
	ArrayOID           uint32
	ElementOID         uint32
	ElementTypeName    string
	Column             string
	Generator          ArrayGenerator
	MinCount           int
	MaxCount           int
}

// ArrayTransformer applies an element transformer to every element of a
// one-dimensional postgres array.
type ArrayTransformer struct {
	inner           Transformer
	arrayOID        uint32
	elementTypeName string
	column          string
	generator       ArrayGenerator
	minCount        int
	maxCount        int
	// pgtype.Map memoizes its encode plans without synchronisation, and one
	// transformer is shared by every snapshot worker, so each concurrent
	// caller takes its own instance rather than sharing one.
	pgMapPool *pool.Pool[*pgtype.Map]
	// injected by the tests, which cannot assert on a random draw
	randIntN func(n int) int
}

func NewArrayTransformer(cfg ArrayConfig) (*ArrayTransformer, error) {
	if cfg.ElementTransformer == nil {
		return nil, fmt.Errorf("%w: no element transformer", ErrInvalidArrayOptions)
	}

	switch cfg.Generator {
	case ArrayGeneratorMap:
		if cfg.MinCount != 0 || cfg.MaxCount != 0 {
			return nil, fmt.Errorf("%w: min_count and max_count only apply to the %q generator", ErrInvalidArrayOptions, ArrayGeneratorRandom)
		}
	case ArrayGeneratorRandom:
		if cfg.MinCount < 0 || cfg.MaxCount < 0 {
			return nil, fmt.Errorf("%w: min_count and max_count must not be negative", ErrInvalidArrayOptions)
		}
		if cfg.MinCount > cfg.MaxCount {
			return nil, fmt.Errorf("%w: min_count %d is greater than max_count %d", ErrInvalidArrayOptions, cfg.MinCount, cfg.MaxCount)
		}
		if cfg.MaxCount > maxArrayElementCount {
			return nil, fmt.Errorf("%w: max_count %d is above the limit of %d", ErrInvalidArrayOptions, cfg.MaxCount, maxArrayElementCount)
		}
	default:
		return nil, fmt.Errorf("%w: unknown generator %q, expected %q or %q", ErrInvalidArrayOptions, cfg.Generator, ArrayGeneratorMap, ArrayGeneratorRandom)
	}

	// the parser accepts a rule on a user-defined array type on the strength
	// of a catalog lookup, so the transformer has to be able to decode that
	// type too. Building the map here means an unusable rule fails while it is
	// built rather than on every row.
	pgMapPool, err := pool.New(func() (*pgtype.Map, error) {
		return pglib.NewArrayTypeMap(cfg.ArrayOID, cfg.ElementOID, cfg.ElementTypeName)
	})
	if err != nil {
		return nil, fmt.Errorf("%w: column %q: %w", ErrInvalidArrayOptions, cfg.Column, err)
	}

	return &ArrayTransformer{
		inner:           cfg.ElementTransformer,
		arrayOID:        cfg.ArrayOID,
		elementTypeName: cfg.ElementTypeName,
		column:          cfg.Column,
		generator:       cfg.Generator,
		minCount:        cfg.MinCount,
		maxCount:        cfg.MaxCount,
		pgMapPool:       pgMapPool,
		randIntN:        rand.IntN,
	}, nil
}

func (t *ArrayTransformer) Transform(ctx context.Context, value Value) (any, error) {
	elements, shape, err := t.decode(value.TransformValue)
	if err != nil {
		return nil, err
	}

	transformed, err := t.transformElements(ctx, elements, value)
	if err != nil {
		return nil, err
	}

	return t.encode(transformed, shape)
}

func (t *ArrayTransformer) decode(value any) ([]any, arrayShape, error) {
	switch v := value.(type) {
	case []any:
		return v, shapeSlice, nil
	case string:
		elements, err := t.decodeLiteral([]byte(v))
		return elements, shapeString, err
	case []byte:
		elements, err := t.decodeLiteral(v)
		return elements, shapeBytes, err
	default:
		return nil, shapeSlice, fmt.Errorf("column %q: expected an array literal or a slice, got %T: %w", t.column, value, ErrUnsupportedValueType)
	}
}

// decodeLiteral scans into pgtype.Array rather than into a slice, because
// pgtype.Array preserves the dimensions pgx parsed. Scanning into a slice
// flattens a nested literal into a single dimension and reports no error, so
// the shape would be lost before the element wise transform sees it.
func (t *ArrayTransformer) decodeLiteral(literal []byte) ([]any, error) {
	pgMap, err := t.pgMapPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("column %q: acquiring a type map: %w", t.column, err)
	}
	defer t.pgMapPool.Release(pgMap)

	var array pgtype.Array[any]
	if err := pgMap.PlanScan(t.arrayOID, pgtype.TextFormatCode, &array).Scan(literal, &array); err != nil {
		return nil, fmt.Errorf("column %q: decoding array literal: %w", t.column, err)
	}
	if len(array.Dims) > 1 {
		return nil, fmt.Errorf("column %q: %w", t.column, ErrMultiDimensionalArray)
	}
	return array.Elements, nil
}

func (t *ArrayTransformer) encode(elements []any, shape arrayShape) (any, error) {
	if shape == shapeSlice {
		return elements, nil
	}

	pgMap, err := t.pgMapPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("column %q: acquiring a type map: %w", t.column, err)
	}
	defer t.pgMapPool.Release(pgMap)

	literal, err := pgMap.Encode(t.arrayOID, pgtype.TextFormatCode, elements, nil)
	if err != nil {
		return nil, fmt.Errorf("column %q: encoding array literal: %w", t.column, err)
	}
	if shape == shapeBytes {
		return literal, nil
	}
	return string(literal), nil
}

func (t *ArrayTransformer) transformElements(ctx context.Context, elements []any, value Value) ([]any, error) {
	if t.generator == ArrayGeneratorRandom {
		return t.resampleElements(ctx, elements, value)
	}

	transformed := make([]any, len(elements))
	for i, element := range elements {
		var err error
		if transformed[i], err = t.transformElement(ctx, element, value); err != nil {
			return nil, fmt.Errorf("column %q: element %d: %w", t.column, i, err)
		}
	}
	return transformed, nil
}

// resampleElements emits a random number of elements, each the transform of a
// source element chosen at random with replacement. Every invocation is handed
// a value that was in the row, so the generator never invents one.
func (t *ArrayTransformer) resampleElements(ctx context.Context, elements []any, value Value) ([]any, error) {
	if len(elements) == 0 {
		return []any{}, nil
	}

	count := t.minCount
	if t.maxCount > t.minCount {
		count += t.randIntN(t.maxCount - t.minCount + 1)
	}

	transformed := make([]any, count)
	for i := range transformed {
		var err error
		if transformed[i], err = t.transformElement(ctx, elements[t.randIntN(len(elements))], value); err != nil {
			return nil, fmt.Errorf("column %q: element %d: %w", t.column, i, err)
		}
	}
	return transformed, nil
}

// transformElement keeps a NULL element NULL without calling the element
// transformer, mirroring how the wal transformer skips a NULL column value. An
// element transformer that returns nil produces a NULL element in place, so
// the length of a mapped array is always preserved.
func (t *ArrayTransformer) transformElement(ctx context.Context, element any, value Value) (any, error) {
	if element == nil {
		return nil, nil
	}
	return t.inner.Transform(ctx, Value{
		TransformValue: element,
		TransformType:  t.elementTypeName,
		DynamicValues:  value.DynamicValues,
	})
}

func (t *ArrayTransformer) CompatibleTypes() []SupportedDataType {
	return t.inner.CompatibleTypes()
}

func (t *ArrayTransformer) Type() TransformerType { return t.inner.Type() }

func (t *ArrayTransformer) IsDynamic() bool { return t.inner.IsDynamic() }

// Uniqueness classifies the whole array transform, which is what validation
// against a unique index needs. Mapping a length preserving, injective element
// transform over an array is injective, so map inherits the element
// transformer's classification verbatim.
func (t *ArrayTransformer) Uniqueness() Uniqueness {
	if t.generator == ArrayGeneratorRandom {
		return UniquenessLossy
	}
	return UniquenessOf(t.inner)
}

func (t *ArrayTransformer) Close() error {
	return t.inner.Close()
}
