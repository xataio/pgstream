// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"github.com/spf13/viper"
)

func pgURL() string {
	pgurl := viper.GetString("pgurl")
	if pgurl != "" {
		return pgurl
	}
	return viper.GetString("PGSTREAM_POSTGRES_LISTENER_URL")
}
