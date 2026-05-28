// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"strings"

	units "github.com/docker/go-units"
	mapstructure "github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

// byteSize represents a size in bytes that can be configured either as a plain
// integer or as a human-readable string with a unit suffix (e.g. "64MiB" or
// "1GiB"). Units are case-insensitive and interpreted as binary multiples
// (1MiB = 1024*1024 bytes).
type byteSize int64

func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	n, err := units.RAMInBytes(s)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
	}
	return n, nil
}

func getByteSize(key string) (int64, error) {
	n, err := parseByteSize(viper.GetString(key))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

// byteSizeDecodeHook builds the mapstructure hook used to decode the YAML
// config. viper.DecodeHook replaces viper's default hook rather than appending
// to it, so the duration and slice hooks are re-included here alongside the
// text unmarshaller hook that decodes byteSize fields.
func byteSizeDecodeHook() mapstructure.DecodeHookFunc {
	return mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
		mapstructure.TextUnmarshallerHookFunc(),
	)
}

func (b *byteSize) UnmarshalText(text []byte) error {
	n, err := parseByteSize(string(text))
	if err != nil {
		return err
	}
	*b = byteSize(n)
	return nil
}

func (b *byteSize) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("invalid byte size %q: expected a scalar value", value.Value)
	}
	n, err := parseByteSize(value.Value)
	if err != nil {
		return err
	}
	*b = byteSize(n)
	return nil
}
