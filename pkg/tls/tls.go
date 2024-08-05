// SPDX-License-Identifier: Apache-2.0

package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type Config struct {
	// Enabled determines if TLS should be used. Defaults to false.
	Enabled bool
	// File path to the CA PEM certificate or PEM certificate to be used for TLS
	// connection. If TLS is enabled and no CA cert file is provided, the system
	// certificate pool is used as default.
	CaCertFile string
	CaCertPEM  string
	// File path to the client PEM certificate or client PEM certificate
	ClientCertFile string
	ClientCertPEM  string
	// File path to the client PEM key or client PEM key content
	ClientKeyFile string
	ClientKeyPEM  string
}

func NewConfig(cfg *Config) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	certPool, err := getCertPool(cfg)
	if err != nil {
		return nil, err
	}

	certificates, err := getCertificates(cfg)
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

func getCertPool(cfg *Config) (*x509.CertPool, error) {
	pemCertBytes, err := readPEMBytes(cfg.CaCertFile, cfg.CaCertPEM)
	if err != nil {
		return nil, fmt.Errorf("reading CA certificate file: %w", err)
	}

	if len(pemCertBytes) == 0 {
		return x509.SystemCertPool()
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(pemCertBytes)
	return certPool, nil
}

func getCertificates(cfg *Config) ([]tls.Certificate, error) {
	if cfg.IsClientCertProvided() {
		pemCertBytes, err := readPEMBytes(cfg.ClientCertFile, cfg.ClientCertPEM)
		if err != nil {
			return nil, err
		}
		pemKeyBytes, err := readPEMBytes(cfg.ClientKeyFile, cfg.ClientKeyPEM)
		if err != nil {
			return nil, err
		}
		cert, err := tls.X509KeyPair(pemCertBytes, pemKeyBytes)
		if err != nil {
			return nil, err
		}
		return []tls.Certificate{cert}, nil
	}

	return []tls.Certificate{}, nil
}

// readPEMBytes will parse the certificate on input and return the pem byte
// content. It accepts a pem certificate or the file path to a pem certificate.
func readPEMBytes(certFile, certPEM string) ([]byte, error) {
	if certFile != "" {
		return os.ReadFile(certFile)
	}

	return []byte(certPEM), nil
}

func (c *Config) IsClientCertProvided() bool {
	return (c.ClientCertFile != "" || c.ClientCertPEM != "") && (c.ClientKeyFile != "" || c.ClientKeyPEM != "")
}
