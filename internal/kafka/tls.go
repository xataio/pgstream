// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type TLSConfig struct {
	Enabled    bool
	CaCert     *x509.CertPool
	ClientCert []byte
	ClientKey  []byte
}

// newTLSConfig builds a tls.Config using the CA and client certificates defined in configuration
func newTLSConfig(config *TLSConfig) (*tls.Config, error) {
	tlsConfig := tls.Config{MinVersion: tls.VersionTLS12, MaxVersion: 0}

	cert, err := tls.X509KeyPair(config.ClientCert, config.ClientKey)
	if err == nil { // Client certificates may or may not be required depending on the broker's configuration
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	tlsConfig.RootCAs = config.CaCert

	return &tlsConfig, nil
}

// BuildDialer builds a TLS kafka.Dialer using the CA certificate defined in configuration
func buildTLSDialer(config *TLSConfig, timeout time.Duration) (*kafka.Dialer, error) {
	tlsConfig, err := newTLSConfig(config)
	if err != nil {
		return nil, fmt.Errorf("loading TLS configuraion: %w", err)
	}

	return &kafka.Dialer{
		Timeout:   timeout,
		DualStack: true,
		TLS:       tlsConfig,
	}, nil
}
