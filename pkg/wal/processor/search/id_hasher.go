// SPDX-License-Identifier: Apache-2.0

package search

import (
	"crypto/sha256"
	"encoding/hex"
)

type IDHasher func(id string) string

func DefaultIDHasher() IDHasher {
	return func(id string) string {
		hash := sha256.Sum256([]byte(id))
		return hex.EncodeToString(hash[:])
	}
}
