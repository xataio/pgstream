// SPDX-License-Identifier: Apache-2.0

package nats

import (
	"fmt"

	"github.com/nats-io/nats.go"
	tlslib "github.com/xataio/pgstream/pkg/tls"
)

func buildConnOptions(cfg *ConnConfig) ([]nats.Option, error) {
	opts := []nats.Option{
		nats.MaxReconnects(-1),
	}

	if cfg.TLS.Enabled {
		tlsConfig, err := tlslib.NewConfig(&cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("building TLS config: %w", err)
		}
		if tlsConfig != nil {
			opts = append(opts, nats.Secure(tlsConfig))
		}
	}

	if cfg.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.CredentialsFile))
	}

	return opts, nil
}
