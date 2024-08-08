// SPDX-License-Identifier: Apache-2.0

package tls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

func Test_NewConfig(t *testing.T) {
	t.Parallel()

	systemCAs, err := x509.SystemCertPool()
	require.NoError(t, err)

	testPEMBytes, err := os.ReadFile("test/test.pem")
	require.NoError(t, err)
	testCertPool := x509.NewCertPool()
	testCertPool.AppendCertsFromPEM(testPEMBytes)

	testClientKeyPair, err := tls.LoadX509KeyPair("test/test.pem", "test/test.key")
	require.NoError(t, err)

	tests := []struct {
		name string
		cfg  *Config

		wantConfig *tls.Config
		wantErr    error
	}{
		{
			name: "ok - tls not enabled",
			cfg: &Config{
				Enabled: false,
			},

			wantConfig: nil,
			wantErr:    nil,
		},
		{
			name: "ok - tls enabled no certificates",
			cfg: &Config{
				Enabled: true,
			},

			wantConfig: &tls.Config{
				MinVersion:   tls.VersionTLS12,
				MaxVersion:   0,
				Certificates: []tls.Certificate{},
				RootCAs:      systemCAs,
			},
			wantErr: nil,
		},
		{
			name: "ok - tls enabled with CA certificate",
			cfg: &Config{
				Enabled:    true,
				CaCertFile: "test/test.pem",
			},

			wantConfig: &tls.Config{
				MinVersion:   tls.VersionTLS12,
				MaxVersion:   0,
				Certificates: []tls.Certificate{},
				RootCAs:      testCertPool,
			},
			wantErr: nil,
		},
		{
			name: "ok - tls enabled with client certificate",
			cfg: &Config{
				Enabled:        true,
				ClientCertFile: "test/test.pem",
				ClientKeyFile:  "test/test.key",
			},

			wantConfig: &tls.Config{
				MinVersion:   tls.VersionTLS12,
				MaxVersion:   0,
				Certificates: []tls.Certificate{testClientKeyPair},
				RootCAs:      systemCAs,
			},
			wantErr: nil,
		},
		{
			name: "ok - tls enabled with CA and client certificate",
			cfg: &Config{
				Enabled:        true,
				CaCertFile:     "test/test.pem",
				ClientCertFile: "test/test.pem",
				ClientKeyFile:  "test/test.key",
			},

			wantConfig: &tls.Config{
				MinVersion:   tls.VersionTLS12,
				MaxVersion:   0,
				Certificates: []tls.Certificate{testClientKeyPair},
				RootCAs:      testCertPool,
			},
			wantErr: nil,
		},
		{
			name: "error - invalid CA certificate file",
			cfg: &Config{
				Enabled:    true,
				CaCertFile: "test/doesnotexist.pem",
			},

			wantConfig: nil,
			wantErr:    os.ErrNotExist,
		},
		{
			name: "error - invalid client certificate file",
			cfg: &Config{
				Enabled:        true,
				ClientCertFile: "test/doesnotexist.pem",
				ClientKeyFile:  "test/test.pem",
			},

			wantConfig: nil,
			wantErr:    os.ErrNotExist,
		},
		{
			name: "error - invalid client key file",
			cfg: &Config{
				Enabled:        true,
				ClientCertFile: "test/test.pem",
				ClientKeyFile:  "test/doesnotexist.pem",
			},

			wantConfig: nil,
			wantErr:    os.ErrNotExist,
		},
		{
			name: "error - invalid client key pair",
			cfg: &Config{
				Enabled:        true,
				ClientCertFile: "test/test.pem",
				ClientKeyFile:  "test/test.pem",
			},

			wantConfig: nil,
			wantErr:    errors.New("tls: found a certificate rather than a key in the PEM for the private key"),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tlsCfg, err := NewConfig(tc.cfg)
			if !errors.Is(err, tc.wantErr) {
				require.EqualError(t, err, tc.wantErr.Error())
			}
			require.Equal(t, "", cmp.Diff(tlsCfg, tc.wantConfig, cmpopts.IgnoreUnexported(tls.Config{}))) //nolint:gosec
		})
	}
}

func Test_readPEMBytes(t *testing.T) {
	t.Parallel()

	testPEMBytes, err := os.ReadFile("test/test.pem")
	require.NoError(t, err)

	tests := []struct {
		name string
		file string
		pem  string

		wantBytes []byte
		wantErr   error
	}{
		{
			name: "with file",
			file: "test/test.pem",

			wantBytes: testPEMBytes,
			wantErr:   nil,
		},
		{
			name: "with pem",
			pem:  string(testPEMBytes),

			wantBytes: testPEMBytes,
			wantErr:   nil,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pemBytes, err := readPEMBytes(tc.file, tc.pem)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantBytes, pemBytes)
		})
	}
}
