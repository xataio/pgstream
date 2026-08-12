// SPDX-License-Identifier: Apache-2.0

// Package objecttype provides include/exclude filtering over user-facing
// database object type categories. Callers supply the mapping from category
// name to the concrete type values used by their source of truth (pg_dump TOC
// types, DDL event object types, ...), and get back the set of categories and
// types that must be excluded.
package objecttype

import (
	"errors"
	"fmt"
)

var (
	ErrIncludeExcludeConflict = errors.New("include and exclude object type lists cannot both be set")
	ErrUnknownCategory        = errors.New("unknown object type category")
)

// Filter reports which object type categories, and which of the type values
// they map to, should be excluded. A nil Filter excludes nothing.
type Filter struct {
	excludedTypes      map[string]struct{}
	excludedCategories map[string]struct{}
}

// NewFilter builds a Filter from the given include/exclude category lists,
// validated against the categories mapping. Only one of include or exclude can
// be set. Setting include excludes every category that is not listed. It
// returns a nil Filter when neither list is set, meaning no filtering.
func NewFilter(categories map[string][]string, include, exclude []string) (*Filter, error) {
	if len(include) > 0 && len(exclude) > 0 {
		return nil, ErrIncludeExcludeConflict
	}

	if len(include) == 0 && len(exclude) == 0 {
		return nil, nil
	}

	f := &Filter{
		excludedTypes:      make(map[string]struct{}),
		excludedCategories: make(map[string]struct{}),
	}

	if len(include) > 0 {
		includedSet := make(map[string]struct{}, len(include))
		for _, cat := range include {
			if _, ok := categories[cat]; !ok {
				return nil, fmt.Errorf("%w: %q", ErrUnknownCategory, cat)
			}
			includedSet[cat] = struct{}{}
		}
		for cat := range categories {
			if _, included := includedSet[cat]; !included {
				f.exclude(cat, categories[cat])
			}
		}
		return f, nil
	}

	for _, cat := range exclude {
		types, ok := categories[cat]
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrUnknownCategory, cat)
		}
		f.exclude(cat, types)
	}

	return f, nil
}

func (f *Filter) exclude(category string, types []string) {
	f.excludedCategories[category] = struct{}{}
	for _, t := range types {
		f.excludedTypes[t] = struct{}{}
	}
}

// IsTypeExcluded returns true if the given type value belongs to an excluded
// category.
func (f *Filter) IsTypeExcluded(objectType string) bool {
	if f == nil {
		return false
	}
	_, excluded := f.excludedTypes[objectType]
	return excluded
}

// IsCategoryExcluded returns true if the given user-facing category is
// excluded.
func (f *Filter) IsCategoryExcluded(category string) bool {
	if f == nil {
		return false
	}
	_, excluded := f.excludedCategories[category]
	return excluded
}
