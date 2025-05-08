// SPDX-License-Identifier: Apache-2.0

package otel

import (
	"runtime/debug"
	"sync"
)

// commitOnce returns either unknownCommitName or the git commit hash for the code that built the
// binary. NOTE: `vcs.revision`, used below, is not available unless one does a `go build` and
// specifically a `go build cmd/xerver` rather than `go build cmd/xerver/*.go`.
var commitOnce = sync.OnceValue(func() string {
	const unknownCommitName = "unknown"

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return unknownCommitName
	}

	for _, v := range info.Settings {
		if v.Key == "vcs.revision" {
			return v.Value
		}
	}

	return unknownCommitName
})

func version() string {
	return commitOnce()
}
