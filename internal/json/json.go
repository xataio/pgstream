// SPDX-License-Identifier: Apache-2.0

package json

import (
	json "github.com/bytedance/sonic"
)

func Unmarshal(b []byte, v any) error {
	return json.Unmarshal(b, v)
}

func Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}
