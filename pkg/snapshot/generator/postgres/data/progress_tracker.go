// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"github.com/xataio/pgstream/internal/progress"
	synclib "github.com/xataio/pgstream/internal/sync"
)

// progressTracker owns the per-schema progress bars shared between the snapshot
// generator, which creates and completes the bars, and the table readers, which
// advance them as rows are processed. The enabled flag and the bars map always
// travel together, so they are grouped here instead of being threaded as a pair
// through every constructor.
//
// The bars map is a pointer, so copies of a progressTracker value share the same
// underlying bars. A zero-value tracker is disabled and every method is a no-op,
// which lets callers advance progress without guarding each call.
type progressTracker struct {
	enabled bool
	bars    *synclib.Map[string, progress.Bar]
}

func newProgressTracker() progressTracker {
	return progressTracker{
		enabled: true,
		bars:    synclib.NewMap[string, progress.Bar](),
	}
}

// set registers the bar tracking the given schema's progress.
func (p progressTracker) set(schema string, bar progress.Bar) {
	if !p.enabled {
		return
	}
	p.bars.Set(schema, bar)
}

// advance adds the given number of bytes to the given schema's bar, if one is
// being tracked.
func (p progressTracker) advance(schema string, bytes int64) {
	if !p.enabled {
		return
	}
	if bar, found := p.bars.Get(schema); found {
		bar.Add64(bytes)
	}
}

// complete closes the given schema's bar and stops tracking it.
func (p progressTracker) complete(schema string) {
	if !p.enabled {
		return
	}
	if bar, found := p.bars.Get(schema); found {
		bar.Close()
	}
	p.bars.Delete(schema)
}
