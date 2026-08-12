// SPDX-License-Identifier: Apache-2.0

package transformers

// validated against unique indexes
type Uniqueness int

const (
	// zero value, so a transformer that never classified itself is
	// distinguishable from one deliberately classified lossy
	UniquenessUnspecified Uniqueness = iota
	UniquenessLossy
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
		return "unspecified"
	}
}
