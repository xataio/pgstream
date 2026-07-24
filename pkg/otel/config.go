// SPDX-License-Identifier: Apache-2.0

package otel

import "time"

type Config struct {
	Metrics *MetricsConfig
	Traces  *TracesConfig
}

type MetricsConfig struct {
	Endpoint           string
	CollectionInterval time.Duration
	Prometheus         *PrometheusConfig
}

type PrometheusConfig struct {
	Enabled  bool
	Endpoint string
}

type TracesConfig struct {
	Endpoint    string
	SampleRatio float64
}

const defaultCollectionInterval = 60 * time.Second

func (c *MetricsConfig) collectionInterval() time.Duration {
	if c.CollectionInterval != 0 {
		return c.CollectionInterval
	}
	return defaultCollectionInterval
}
