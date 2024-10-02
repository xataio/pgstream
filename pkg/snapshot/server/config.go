// SPDX-License-Identifier: Apache-2.0

package server

type Config struct {
	// Port in which the grpc server will be listening. Defaults to 9800
	Port int
}

const defaultPort = 9800

func (c *Config) port() int {
	if c.Port > 0 {
		return c.Port
	}
	return defaultPort
}
