// SPDX-License-Identifier: Apache-2.0

package snapshot

type Snapshot struct {
	SchemaName          string
	TableName           string
	IdentityColumnNames []string
	Status              Status
	Error               error
}

type Status string

const (
	StatusRequested  = Status("requested")
	StatusInProgress = Status("in progress")
	StatusCompleted  = Status("completed")
)

func (s *Snapshot) IsValid() bool {
	return s != nil && s.SchemaName != "" && s.TableName != "" && len(s.IdentityColumnNames) > 0
}

func (s *Snapshot) MarkCompleted(err error) {
	s.Status = StatusCompleted
	s.Error = err
}

func (s *Snapshot) MarkInProgress() {
	s.Status = StatusInProgress
}
