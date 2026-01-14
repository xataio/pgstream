// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"
	"sync"
	"time"
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
}

// defaultCacheTTL is the default time-to-live for the replication metrics
// cache. The value of 30 seconds guarantees that the metrics are queried at
// most every half a minute, which aligns with the typical interval for metric
// collection (30-60s).
const defaultCacheTTL = 30 * time.Second

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
// updates the cache.
func (c *metricsCache) GetReplicationLag(ctx context.Context) (int64, error) {
	if c.isReplicationLagCacheValid() {
		return c.replicationLag, nil
	}

	lag, err := c.inner.GetReplicationLag(ctx)
	if err != nil {
		return 0, err
	}

	c.setReplicationLagCache(lag)
	return lag, nil
}

// isReplicationLagCacheValid checks if the cached replication lag is still valid
// based on the configured TTL.
func (c *metricsCache) isReplicationLagCacheValid() bool {
	c.replicationLagMutex.RLock()
	defer c.replicationLagMutex.RUnlock()

	return !c.replicationLagUpdatedAt.IsZero() && time.Since(c.replicationLagUpdatedAt) < c.ttl
}

// setReplicationLagCache updates the cached replication lag and its timestamp.
func (c *metricsCache) setReplicationLagCache(lag int64) {
	c.replicationLagMutex.Lock()
	defer c.replicationLagMutex.Unlock()

	c.replicationLag = lag
	c.replicationLagUpdatedAt = time.Now()
}
