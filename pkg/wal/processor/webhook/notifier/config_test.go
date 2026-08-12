// SPDX-License-Identifier: Apache-2.0

package notifier

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/backoff"
)

func TestConfig_backoffConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config Config
		want   *backoff.Config
	}{
		{
			name:   "no backoff configured falls back to the default exponential policy",
			config: Config{},
			want: &backoff.Config{
				Exponential: &backoff.ExponentialConfig{
					InitialInterval: defaultBackoffInitialInterval,
					MaxInterval:     defaultBackoffMaxInterval,
					MaxRetries:      defaultBackoffMaxRetries,
				},
			},
		},
		{
			name: "DisableRetries disables retries regardless of any configured policy",
			config: Config{
				Backoff: backoff.Config{
					DisableRetries: true,
					Exponential: &backoff.ExponentialConfig{
						MaxRetries: 5,
					},
				},
			},
			want: &backoff.Config{DisableRetries: true},
		},
		{
			name: "fully configured exponential policy is used as-is",
			config: Config{
				Backoff: backoff.Config{
					Exponential: &backoff.ExponentialConfig{
						InitialInterval: 2 * time.Second,
						MaxInterval:     time.Minute,
						MaxRetries:      7,
					},
				},
			},
			want: &backoff.Config{
				Exponential: &backoff.ExponentialConfig{
					InitialInterval: 2 * time.Second,
					MaxInterval:     time.Minute,
					MaxRetries:      7,
				},
			},
		},
		{
			name: "exponential policy with no interval configured is defaulted to avoid a zero-delay retry storm",
			config: Config{
				Backoff: backoff.Config{
					Exponential: &backoff.ExponentialConfig{
						MaxRetries: 5,
					},
				},
			},
			want: &backoff.Config{
				Exponential: &backoff.ExponentialConfig{
					InitialInterval: defaultBackoffInitialInterval,
					MaxInterval:     defaultBackoffMaxInterval,
					MaxRetries:      5,
				},
			},
		},
		{
			name: "exponential policy with MaxRetries left unset is not defaulted, since 0 means unlimited retries",
			config: Config{
				Backoff: backoff.Config{
					Exponential: &backoff.ExponentialConfig{
						InitialInterval: time.Second,
						MaxInterval:     time.Minute,
					},
				},
			},
			want: &backoff.Config{
				Exponential: &backoff.ExponentialConfig{
					InitialInterval: time.Second,
					MaxInterval:     time.Minute,
					MaxRetries:      0,
				},
			},
		},
		{
			name: "constant policy with no interval configured is defaulted to avoid a zero-delay retry storm",
			config: Config{
				Backoff: backoff.Config{
					Constant: &backoff.ConstantConfig{
						MaxRetries: 5,
					},
				},
			},
			want: &backoff.Config{
				Constant: &backoff.ConstantConfig{
					Interval:   defaultBackoffInitialInterval,
					MaxRetries: 5,
				},
			},
		},
		{
			name: "fully configured constant policy is used as-is",
			config: Config{
				Backoff: backoff.Config{
					Constant: &backoff.ConstantConfig{
						Interval:   5 * time.Second,
						MaxRetries: 5,
					},
				},
			},
			want: &backoff.Config{
				Constant: &backoff.ConstantConfig{
					Interval:   5 * time.Second,
					MaxRetries: 5,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tc.config.backoffConfig()
			require.Equal(t, tc.want, got)
		})
	}
}
