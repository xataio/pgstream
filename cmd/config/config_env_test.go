// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_EnvConfigToStreamConfig(t *testing.T) {
	require.NoError(t, LoadFile("test/test_config.env"))

	streamConfig, err := envConfigToStreamConfig()
	assert.NoError(t, err)
	assert.NotNil(t, streamConfig)

	validateTestStreamConfig(t, streamConfig)
}
