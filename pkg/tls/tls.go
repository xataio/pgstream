// SPDX-License-Identifier: Apache-2.0

package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
)

type Config struct {
	// Enabled determines if TLS should be used. Defaults to false.
	Enabled bool
	// File path to the CA PEM certificate or PEM certificate to be used for TLS
	// connection. If TLS is enabled and no CA cert file is provided, the system
	// certificate pool is used as default.
	CaCertFile string
	// File path to the client PEM certificate or client PEM certificate
	ClientCertFile string
	// File path to the client PEM key or client PEM key content
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
		pemCertBytes, err := readPEMBytes(caCertFile)
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
		pemCertBytes, err := readPEMBytes(clientCertFile)
		if err != nil {
			return nil, err
		}
		pemKeyBytes, err := readPEMBytes(clientKeyFile)
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
func readPEMBytes(cert string) ([]byte, error) {
	if isPEMString(cert) {
		return []byte(cert), nil
	}

	return os.ReadFile(cert)
}

// isPEMString returns true if the provided string match a PEM formatted certificate.
func isPEMString(s string) bool {
	// trim the certificates to make sure we tolerate any yaml weirdness
	trimmedStr := strings.TrimSpace(s)

	// we assume that the string starts with "-" and let further validation
	// verifies the PEM format. When migrating from pkcs12 to PEM a "Bag
	// Attributes" header is added.
	return strings.HasPrefix(trimmedStr, "-") || strings.HasPrefix(trimmedStr, "Bag Attributes")
}
