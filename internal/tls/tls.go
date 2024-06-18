// SPDX-License-Identifier: Apache-2.0

package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
)

type Config struct {
	// Enabled determines if TLS should be used. Defaults to false.
	Enabled bool
	// File path to the CA PEM certificate to be used for TLS connection. If TLS is
	// enabled and no CA cert file is provided, the system certificate pool is
	// used as default.
	CaCertFile string
	// File path to the client PEM certificate
	ClientCertFile string
	// File path to the client PEM key
	ClientKeyFile string
}

func NewConfig(cfg *Config) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	certPool, err := getCertPool(cfg.CaCertFile)
	if err != nil {
		return nil, err
	}

	certificates, err := getCertificates(cfg.ClientCertFile, cfg.ClientKeyFile)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   0,
		Certificates: certificates,
		RootCAs:      certPool,
	}, nil
}

func getCertPool(caCertFile string) (*x509.CertPool, error) {
	if caCertFile != "" {
		pemCertBytes, err := readFile(caCertFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA certificate file: %w", err)
		}
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(pemCertBytes)
		return certPool, nil
	}

	return x509.SystemCertPool()
}

func getCertificates(clientCertFile, clientKeyFile string) ([]tls.Certificate, error) {
	if clientCertFile != "" && clientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, err
		}
		return []tls.Certificate{cert}, nil
	}

	return []tls.Certificate{}, nil
}

func readFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return io.ReadAll(file)
}
