// SPDX-License-Identifier: Apache-2.0

package profiling

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
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
