// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"fmt"
	"strings"
)

type errSingleCharName struct {
	Details string
}

func (e *errSingleCharName) Error() string {
	return fmt.Sprintf("no single character naming candidate: %s", e.Details)
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if strings.HasPrefix(err.Error(), "unable to find candidates with range") && strings.HasSuffix(err.Error(), ":1]") {
		return &errSingleCharName{Details: err.Error()}
	}
	return fmt.Errorf("neosync_transformer: %w", err)
}
