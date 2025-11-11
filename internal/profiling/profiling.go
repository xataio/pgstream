// SPDX-License-Identifier: Apache-2.0

package profiling

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	loglib "github.com/xataio/pgstream/pkg/log"
)

// StartProfilingServer starts an http server exposing /debug/pprof endpoint
// with profiling insights.
func StartProfilingServer(address string) {
	// by adding _ "net/http/pprof" the profiling endpoint attaches to the
	// server
	go func() {
		http.ListenAndServe(address, nil) //nolint:gosec
	}()
}

func StartCPUProfile(fileName string) (func(), error) {
	if fileName == "" {
		fileName = "cpu.prof"
	}
	cpuFile, err := os.Create(fileName)
	if err != nil {
		return nil, fmt.Errorf("could not create CPU profile file: %w", err)
	}

	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		return nil, fmt.Errorf("could not start CPU profile: %w", err)
	}

	return func() {
		pprof.StopCPUProfile()
		cpuFile.Close()
	}, nil
}

func CreateMemoryProfile(fileName string) error {
	if fileName == "" {
		fileName = "mem.prof"
	}
	memFile, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("could not create memory profile file: %w", err)
	}
	defer memFile.Close()

	runtime.GC() // get up-to-date statistics
	// Lookup("allocs") creates a profile similar to go test -memprofile.
	// Alternatively, use Lookup("heap") for a profile
	// that has inuse_space as the default index.
	if err := pprof.Lookup("allocs").WriteTo(memFile, 0); err != nil {
		return fmt.Errorf("could not write memory profile: %w", err)
	}

	return nil
}

// StartPeriodicGoroutineDump starts a goroutine that periodically dumps
// the full stacktrace of all goroutines to the logger. It respects context
// cancellation for clean shutdown.
func StartPeriodicGoroutineDump(ctx context.Context, interval time.Duration, logger loglib.Logger) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Info("stopping periodic goroutine dump")
				return
			case <-ticker.C:
				dumpGoroutineStacktrace(logger)
			}
		}
	}()
}

func dumpGoroutineStacktrace(logger loglib.Logger) {
	var buf bytes.Buffer
	profile := pprof.Lookup("goroutine")
	if profile == nil {
		logger.Error(nil, "failed to lookup goroutine profile")
		return
	}

	// Write the full stacktrace (debug=2 gives full stack traces)
	if err := profile.WriteTo(&buf, 2); err != nil {
		logger.Error(err, "failed to write goroutine stacktrace")
		return
	}

	logger.Info("periodic goroutine stacktrace dump", loglib.Fields{
		"goroutine_count": runtime.NumGoroutine(),
		"stacktrace":      buf.String(),
	})
}
