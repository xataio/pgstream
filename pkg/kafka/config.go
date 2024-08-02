// SPDX-License-Identifier: Apache-2.0

package kafka

import tlslib "github.com/xataio/pgstream/pkg/tls"

type ConnConfig struct {
	Servers []string
	Topic   TopicConfig
	TLS     tlslib.Config
}

type TopicConfig struct {
	Name string
	// Number of partitions to be created for the topic. Defaults to 1.
	NumPartitions int
	// Replication factor for the topic. Defaults to 1.
	ReplicationFactor int
	// AutoCreate defines if the topic should be created if it doesn't exist.
	// Defaults to false.
	AutoCreate bool
}

const (
	defaultNumPartitions     = 1
	defaultReplicationFactor = 1
)

func (c *TopicConfig) numPartitions() int {
	if c.NumPartitions > 0 {
		return c.NumPartitions
	}
	return defaultNumPartitions
}

func (c *TopicConfig) replicationFactor() int {
	if c.NumPartitions > 0 {
		return c.ReplicationFactor
	}
	return defaultReplicationFactor
}
