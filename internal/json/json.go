// SPDX-License-Identifier: Apache-2.0

package json

import (
	json "github.com/bytedance/sonic"
)

// useInt64API decodes JSON integers into int64 instead of float64, preserving
// full precision for values whose magnitude exceeds 2^53. Non-integer numbers
// still decode as float64.
var useInt64API = json.Config{UseInt64: true}.Froze()

func Unmarshal(b []byte, v any) error {
	return json.Unmarshal(b, v)
}

// UnmarshalUseInt64 behaves like Unmarshal but decodes JSON integers into
// int64 (rather than float64) when the destination is an interface{}. This
// matters for full-range bigint values from wal2json: float64 only has 53
// bits of mantissa and would silently round.
func UnmarshalUseInt64(b []byte, v any) error {
	return useInt64API.Unmarshal(b, v)
}

// useNumberAPI decodes every JSON number into json.Number, which keeps the
// text the source printed. A float64 holds about 17 digits and a postgres
// numeric holds up to 1000, so a numeric that goes through a float64 comes
// back rounded.
var useNumberAPI = json.Config{UseNumber: true}.Froze()

// UnmarshalUseNumber behaves like Unmarshal but decodes every JSON number into
// json.Number when the destination is an interface{}. The caller then gives
// each value the type its column needs.
func UnmarshalUseNumber(b []byte, v any) error {
	return useNumberAPI.Unmarshal(b, v)
}

func Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}
