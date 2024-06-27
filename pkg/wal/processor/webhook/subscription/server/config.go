// SPDX-License-Identifier: Apache-2.0

package server

import "time"

type Config struct {
	// Address for the server to listen on. The format is "host:port". Defaults
	// to ":9900".
	Address string
	// ReadTimeout is the maximum duration for reading the entire request,
	// including the body. Defaults to 5s.
	ReadTimeout time.Duration
	// WriteTimeout is the maximum duration before timing out writes of the
	// response. It is reset whenever a new request's header is read. Defaults
	// to 10s.
	WriteTimeout time.Duration
}

const (
	defaultServerReadTimeout  = 5 * time.Second
	defaultServerWriteTimeout = 10 * time.Second
	defaultServerAddress      = ":9900"
)

func (c *Config) readTimeout() time.Duration {
	if c.ReadTimeout > 0 {
		return c.ReadTimeout
	}
	return defaultServerReadTimeout
}

func (c *Config) writeTimeout() time.Duration {
	if c.WriteTimeout > 0 {
		return c.WriteTimeout
	}
	return defaultServerWriteTimeout
}

func (c *Config) address() string {
	if c.Address != "" {
		return c.Address
	}
	return defaultServerAddress
}
