// SPDX-License-Identifier: Apache-2.0

package generators

import (
	greenmaskgenerators "github.com/eminano/greenmask/pkg/generators"
	"github.com/xataio/pgstream/pkg/transformers/internal/pool"
)

// pooledGenerator makes a non concurrency-safe greenmask generator safe for
// concurrent use by handing out a dedicated underlying instance per caller.
type pooledGenerator struct {
	pool *pool.Pool[greenmaskgenerators.Generator]
	size int
}

func newPooledGenerator(size int, newFn func() (greenmaskgenerators.Generator, error)) (Generator, error) {
	p, err := pool.New(newFn)
	if err != nil {
		return nil, err
	}
	return &pooledGenerator{pool: p, size: size}, nil
}

func (g *pooledGenerator) Generate(data []byte) ([]byte, error) {
	gen, err := g.pool.Acquire()
	if err != nil {
		return nil, err
	}
	defer g.pool.Release(gen)
	res, err := gen.Generate(data)
	if err != nil {
		return nil, err
	}
	// hash based generators return a slice aliasing their internal buffer, so
	// it must be copied before the instance is released. The copy keeps the
	// full backing array up to capacity, because greenmask's person
	// transformer reads past the returned length into the spare bytes the
	// random generator leaves there.
	out := make([]byte, len(res), cap(res))
	copy(out[:cap(out)], res[:cap(res)])
	return out, nil
}

func (g *pooledGenerator) Size() int {
	return g.size
}
