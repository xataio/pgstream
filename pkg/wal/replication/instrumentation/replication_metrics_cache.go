// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// metricsCache adds a caching layer for the replication metrics to reduce the
// number of calls to the underlying handler (and in turn the database).
type metricsCache struct {
	inner metricRetriever

	// cache layer for replication lag
	ttl                     time.Duration
	replicationLagMutex     sync.RWMutex
	replicationLagUpdatedAt time.Time
	replicationLag          int64

	// singleflight group ensures only one goroutine fetches fresh data when
	// the cache expires, while other concurrent callers wait for that result
	group singleflight.Group
}

// defaultCacheTTL is the default time-to-live for the replication metrics
// cache. The value of 30 seconds guarantees that the metrics are queried at
// most every half a minute, which aligns with the typical interval for metric
// collection (30-60s).
const defaultCacheTTL = 30 * time.Second

var errUnexpectedLagValueType = errors.New("unexpected type for replication lag value")

// newMetricsCache creates a new metricsCache with the specified TTL for the
// replication metrics cache. If the provided TTL is less than or equal to zero,
// a default value is used.
func newMetricsCache(inner metricRetriever, ttl time.Duration) *metricsCache {
	if ttl <= 0 {
		ttl = defaultCacheTTL
	}
	return &metricsCache{
		inner: inner,
		ttl:   ttl,
	}
}

// GetReplicationLag returns the replication lag, using a cached value if it is
// still valid. Otherwise, it fetches a fresh value from the inner retriever and
// updates the cache. If multiple goroutines call this concurrently when the cache
// is invalid, only one will fetch the data while others wait for the result.
func (c *metricsCache) GetReplicationLag(ctx context.Context) (int64, error) {
	if cacheLag, isValid := c.isReplicationLagCacheValid(); isValid {
		return cacheLag, nil
	}

	// Use singleflight to deduplicate concurrent requests when cache is invalid
	val, err, _ := c.group.Do("replication_lag", func() (any, error) {
		lag, err := c.inner.GetReplicationLag(ctx)
		if err != nil {
			return int64(0), err
		}

		c.setReplicationLagCache(lag)
		return lag, nil
	})

	if err != nil {
		return 0, err
	}

	lag, ok := val.(int64)
	if !ok {
		return 0, errUnexpectedLagValueType
	}
	return lag, nil
}

// isReplicationLagCacheValid checks if the cached replication lag is still valid
// based on the configured TTL.
func (c *metricsCache) isReplicationLagCacheValid() (int64, bool) {
	c.replicationLagMutex.RLock()
	defer c.replicationLagMutex.RUnlock()

	isValid := !c.replicationLagUpdatedAt.IsZero() && time.Since(c.replicationLagUpdatedAt) < c.ttl
	return c.replicationLag, isValid
}

// setReplicationLagCache updates the cached replication lag and its timestamp.
func (c *metricsCache) setReplicationLagCache(lag int64) {
	c.replicationLagMutex.Lock()
	defer c.replicationLagMutex.Unlock()

	c.replicationLag = lag
	c.replicationLagUpdatedAt = time.Now()
}
