// SPDX-License-Identifier: Apache-2.0

package search

import (
	"crypto/sha256"
	"encoding/hex"
)

// IDHasher hashes document IDs to stay within Elasticsearch's 512-byte ID limit.
type IDHasher func(id string) string

// DefaultIDHasher returns a SHA256-based hasher producing 64-char hex strings.
func DefaultIDHasher() IDHasher {
	return func(id string) string {
		hash := sha256.Sum256([]byte(id))
		return hex.EncodeToString(hash[:])
	}
}
