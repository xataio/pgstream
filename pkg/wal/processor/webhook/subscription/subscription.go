// SPDX-License-Identifier: Apache-2.0

package subscription

import (
	"fmt"
	"slices"
)

type Subscription struct {
	URL        string   `json:"url"`
	EventTypes []string `json:"event_types"`
	Schema     string   `json:"schema"`
	Table      string   `json:"table"`
}

func (s *Subscription) IsFor(action, schema, table string) bool {
	if action == "" && schema == "" && table == "" {
		return true
	}

	if action != "" && len(s.EventTypes) > 0 && !slices.Contains(s.EventTypes, action) {
		return false
	}

	if schema != "" && s.Schema != "" && s.Schema != schema {
		return false
	}

	if table != "" && s.Table != "" && s.Table != table {
		return false
	}

	return true
}

func (s *Subscription) Key() string {
	return fmt.Sprintf("%s/%s/%s", s.URL, s.Schema, s.Table)
}
