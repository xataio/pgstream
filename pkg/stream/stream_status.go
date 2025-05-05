// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"fmt"
	"strings"
)

type Status struct {
	Init                *InitStatus
	Config              *ConfigStatus
	TransformationRules *TransformationRulesStatus
	Source              *SourceStatus
}

type ConfigStatus struct {
	Valid  bool
	Errors []string
}

type TransformationRulesStatus struct {
	Valid  bool
	Errors []string
}

type SourceStatus struct {
	Reachable bool
	Errors    []string
}

type InitStatus struct {
	PgstreamSchema  *SchemaStatus
	Migration       *MigrationStatus
	ReplicationSlot *ReplicationSlotStatus
}

type MigrationStatus struct {
	Version uint
	Dirty   bool
	Errors  []string
}

type ReplicationSlotStatus struct {
	Name     string
	Plugin   string
	Database string
	Errors   []string
}

type SchemaStatus struct {
	SchemaExists         bool
	SchemaLogTableExists bool
	Errors               []string
}

type StatusErrors map[string][]string

func (se StatusErrors) Keys() []string {
	keys := make([]string, 0, len(se))
	for k := range se {
		keys = append(keys, k)
	}
	return keys
}

func (s *Status) GetErrors() StatusErrors {
	if s == nil {
		return nil
	}

	errors := map[string][]string{}
	if initErrs := s.Init.GetErrors(); len(initErrs) > 0 {
		errors["init"] = initErrs
	}

	if s.Source != nil && len(s.Source.Errors) > 0 {
		errors["source"] = s.Source.Errors
	}

	if s.Config != nil && len(s.Config.Errors) > 0 {
		errors["config"] = s.Config.Errors
	}

	if s.TransformationRules != nil && len(s.TransformationRules.Errors) > 0 {
		errors["transformation_rules"] = s.TransformationRules.Errors
	}

	return errors
}

func (s *Status) PrettyPrint() string {
	if s == nil {
		return ""
	}

	var prettyPrint strings.Builder
	prettyPrint.WriteString(s.Init.PrettyPrint())
	prettyPrint.WriteByte('\n')
	prettyPrint.WriteString(s.Config.PrettyPrint())
	prettyPrint.WriteByte('\n')
	prettyPrint.WriteString(s.TransformationRules.PrettyPrint())
	prettyPrint.WriteByte('\n')
	prettyPrint.WriteString(s.Source.PrettyPrint())

	return prettyPrint.String()
}

// GetErrors aggregates all errors from the initialisation status.
func (is *InitStatus) GetErrors() []string {
	if is == nil {
		return nil
	}

	errors := []string{}
	if is.PgstreamSchema != nil && len(is.PgstreamSchema.Errors) > 0 {
		errors = append(errors, is.PgstreamSchema.Errors...)
	}

	if is.Migration != nil && len(is.Migration.Errors) > 0 {
		errors = append(errors, is.Migration.Errors...)
	}

	if is.ReplicationSlot != nil && len(is.ReplicationSlot.Errors) > 0 {
		errors = append(errors, is.ReplicationSlot.Errors...)
	}

	return errors
}

func (is *InitStatus) PrettyPrint() string {
	if is == nil {
		return ""
	}

	var prettyPrint strings.Builder
	prettyPrint.WriteString("Initialisation status:\n")
	if is.PgstreamSchema != nil {
		prettyPrint.WriteString(fmt.Sprintf(" - Pgstream schema exists: %t\n", is.PgstreamSchema.SchemaExists))
		prettyPrint.WriteString(fmt.Sprintf(" - Pgstream schema_log table exists: %t\n", is.PgstreamSchema.SchemaLogTableExists))
		if len(is.PgstreamSchema.Errors) > 0 {
			prettyPrint.WriteString(fmt.Sprintf(" - Pgstream schema errors: %s\n", is.PgstreamSchema.Errors))
		}
	}

	if is.Migration != nil {
		prettyPrint.WriteString(fmt.Sprintf(" - Migration current version: %d\n", is.Migration.Version))
		prettyPrint.WriteString(fmt.Sprintf(" - Migration status: %s\n", migrationStatus(is.Migration.Dirty)))
		if len(is.Migration.Errors) > 0 {
			prettyPrint.WriteString(fmt.Sprintf(" - Migration errors: %s\n", is.Migration.Errors))
		}
	}

	if is.ReplicationSlot != nil {
		prettyPrint.WriteString(fmt.Sprintf(" - Replication slot name: %s\n", is.ReplicationSlot.Name))
		prettyPrint.WriteString(fmt.Sprintf(" - Replication slot plugin: %s\n", is.ReplicationSlot.Plugin))
		prettyPrint.WriteString(fmt.Sprintf(" - Replication slot database: %s\n", is.ReplicationSlot.Database))
		if len(is.ReplicationSlot.Errors) > 0 {
			prettyPrint.WriteString(fmt.Sprintf(" - Replication slot errors: %s\n", is.ReplicationSlot.Errors))
		}
	}

	// trim the last newline character
	return prettyPrint.String()[:len(prettyPrint.String())-1]
}

func (ss *SourceStatus) PrettyPrint() string {
	if ss == nil {
		return ""
	}

	var prettyPrint strings.Builder
	prettyPrint.WriteString("Source status:\n")
	prettyPrint.WriteString(fmt.Sprintf(" - Reachable: %t\n", ss.Reachable))
	if len(ss.Errors) > 0 {
		prettyPrint.WriteString(fmt.Sprintf(" - Errors: %s\n", ss.Errors))
	}

	// trim the last newline character
	return prettyPrint.String()[:len(prettyPrint.String())-1]
}

func (cs *ConfigStatus) PrettyPrint() string {
	if cs == nil {
		return ""
	}

	var prettyPrint strings.Builder
	prettyPrint.WriteString("Config status:\n")
	prettyPrint.WriteString(fmt.Sprintf(" - Valid: %t\n", cs.Valid))
	if len(cs.Errors) > 0 {
		prettyPrint.WriteString(fmt.Sprintf(" - Errors: %s\n", cs.Errors))
	}

	// trim the last newline character
	return prettyPrint.String()[:len(prettyPrint.String())-1]
}

func (trs *TransformationRulesStatus) PrettyPrint() string {
	if trs == nil {
		return ""
	}

	var prettyPrint strings.Builder
	prettyPrint.WriteString("Transformation rules status:\n")
	prettyPrint.WriteString(fmt.Sprintf(" - Valid: %t\n", trs.Valid))
	if len(trs.Errors) > 0 {
		prettyPrint.WriteString(fmt.Sprintf(" - Errors: %s\n", trs.Errors))
	}

	// trim the last newline character
	return prettyPrint.String()[:len(prettyPrint.String())-1]
}

func migrationStatus(dirty bool) string {
	if !dirty {
		return "success"
	}
	return "failed"
}
