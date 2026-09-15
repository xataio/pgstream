// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/stretchr/testify/require"
)

func TestSASLConfig_mechanism(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  SASLConfig

		wantName string
		wantNil  bool
		wantErr  error
	}{
		{
			name: "ok - sasl not enabled",
			cfg:  SASLConfig{Enabled: false},

			wantNil: true,
			wantErr: nil,
		},
		{
			name: "ok - sasl not enabled with credentials",
			cfg: SASLConfig{
				Enabled:   false,
				Mechanism: "invalid",
				Username:  "myuser",
				Password:  "mypassword",
			},

			wantNil: true,
			wantErr: nil,
		},
		{
			name: "ok - plain",
			cfg: SASLConfig{
				Enabled:   true,
				Mechanism: "plain",
				Username:  "myuser",
				Password:  "mypassword",
			},

			wantName: "PLAIN",
			wantErr:  nil,
		},
		{
			name: "ok - mechanism is case insensitive",
			cfg: SASLConfig{
				Enabled:   true,
				Mechanism: "SCRAM-SHA-256",
				Username:  "myuser",
				Password:  "mypassword",
			},

			wantName: "SCRAM-SHA-256",
			wantErr:  nil,
		},
		{
			name: "ok - scram sha 512",
			cfg: SASLConfig{
				Enabled:   true,
				Mechanism: "scram-sha-512",
				Username:  "myuser",
				Password:  "mypassword",
			},

			wantName: "SCRAM-SHA-512",
			wantErr:  nil,
		},
		{
			name: "error - missing username",
			cfg: SASLConfig{
				Enabled:   true,
				Mechanism: "plain",
				Password:  "mypassword",
			},

			wantErr: errMissingSASLUsername,
		},
		{
			name: "error - missing password",
			cfg: SASLConfig{
				Enabled:   true,
				Mechanism: "plain",
				Username:  "myuser",
			},

			wantErr: errMissingSASLPassword,
		},
		{
			name: "error - missing mechanism",
			cfg: SASLConfig{
				Enabled:  true,
				Username: "myuser",
				Password: "mypassword",
			},

			wantErr: errMissingSASLMechanism,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mechanism, err := tc.cfg.mechanism()
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, mechanism)
				return
			}
			require.NoError(t, err)

			if tc.wantNil {
				require.Nil(t, mechanism)
				return
			}

			require.NotNil(t, mechanism)
			require.Equal(t, tc.wantName, mechanism.Name())
		})
	}
}

func TestSASLConfig_mechanism_unsupported(t *testing.T) {
	t.Parallel()

	cfg := SASLConfig{
		Enabled:   true,
		Mechanism: "oauthbearer",
		Username:  "myuser",
		Password:  "mypassword",
	}

	mechanism, err := cfg.mechanism()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported SASL mechanism [oauthbearer]")
	require.Nil(t, mechanism)
}

func TestSASLConfig_mechanism_plainCredentials(t *testing.T) {
	t.Parallel()

	cfg := SASLConfig{
		Enabled:   true,
		Mechanism: "plain",
		Username:  "myuser",
		Password:  "mypassword",
	}

	mechanism, err := cfg.mechanism()
	require.NoError(t, err)
	require.Equal(t, plain.Mechanism{Username: "myuser", Password: "mypassword"}, mechanism)
}

func TestBuildDialer_SASL(t *testing.T) {
	t.Parallel()

	cfg := &ConnConfig{
		Servers: []string{"localhost:9092"},
		SASL: SASLConfig{
			Enabled:   true,
			Mechanism: "scram-sha-256",
			Username:  "myuser",
			Password:  "mypassword",
		},
	}

	dialer, err := buildDialer(cfg)
	require.NoError(t, err)
	require.NotNil(t, dialer.SASLMechanism)
	require.Equal(t, "SCRAM-SHA-256", dialer.SASLMechanism.Name())

	cfg.SASL.Enabled = false
	dialer, err = buildDialer(cfg)
	require.NoError(t, err)
	require.Nil(t, dialer.SASLMechanism)

	cfg.SASL = SASLConfig{Enabled: true, Username: "myuser", Password: "mypassword"}
	_, err = buildDialer(cfg)
	require.ErrorIs(t, err, errMissingSASLMechanism)
}

func TestBuildTransport_SASL(t *testing.T) {
	t.Parallel()

	cfg := &ConnConfig{
		Servers: []string{"localhost:9092"},
		SASL: SASLConfig{
			Enabled:   true,
			Mechanism: "plain",
			Username:  "myuser",
			Password:  "mypassword",
		},
	}

	roundTripper, err := buildTransport(cfg)
	require.NoError(t, err)
	transport, ok := roundTripper.(*kafka.Transport)
	require.True(t, ok)
	require.NotNil(t, transport.SASL)
	require.Equal(t, "PLAIN", transport.SASL.Name())

	cfg.SASL.Enabled = false
	roundTripper, err = buildTransport(cfg)
	require.NoError(t, err)
	transport, ok = roundTripper.(*kafka.Transport)
	require.True(t, ok)
	require.Nil(t, transport.SASL)

	cfg.SASL = SASLConfig{Enabled: true, Mechanism: "plain", Password: "mypassword"}
	_, err = buildTransport(cfg)
	require.ErrorIs(t, err, errMissingSASLUsername)
}
