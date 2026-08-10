// SPDX-License-Identifier: Apache-2.0

package transformers

// validated against unique indexes
type Uniqueness int

const (
	UniquenessLossy Uniqueness = iota
	UniquenessNotGuaranteed
	UniquenessPreserved
)

func (u Uniqueness) String() string {
	switch u {
	case UniquenessLossy:
		return "lossy"
	case UniquenessNotGuaranteed:
		return "not_guaranteed"
	case UniquenessPreserved:
		return "preserved"
	default:
		return "unknown"
	}
}
