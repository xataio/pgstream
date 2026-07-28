// SPDX-License-Identifier: Apache-2.0

package template

import (
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		template string
		data     any
		wantErr  bool
		wantOut  string
	}{
		{
			name:     "static template",
			template: "hello world",
			wantOut:  "hello world",
		},
		{
			name:     "template with data",
			template: "hello {{ . }}",
			data:     "world",
			wantOut:  "hello world",
		},
		{
			name:     "template with sprig function",
			template: "{{ upper . }}",
			data:     "world",
			wantOut:  "WORLD",
		},
		{
			name:     "template with greenmask function",
			template: `{{ masking "default" . }}`,
			data:     "secret",
			wantOut:  "******",
		},
		{
			name:     "invalid template",
			template: "{{ unknownFunction }}",
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tmpl, err := New("test", tc.template)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			buf := &strings.Builder{}
			require.NoError(t, tmpl.Execute(buf, tc.data))
			require.Equal(t, tc.wantOut, buf.String())
		})
	}
}

// TestTemplate_ConcurrentExecute exercises issue #1043: the greenmask toolkit
// function map closes over a shared rng, so executing one template from
// several goroutines corrupted the generator and failed with "index out of
// range [-1]". Run with the race detector, it also covers the pgtype.Map
// behind the interval functions.
func TestTemplate_ConcurrentExecute(t *testing.T) {
	t.Parallel()

	const (
		numGoroutines = 16
		numIterations = 100
		minValue      = 1
		maxValue      = 1000
	)

	tmpl, err := New("test", `{{ randomInt 1 1000 }} {{ randomFloat 0 1 2 }} {{ randomString 4 8 }} `+
		`{{ randomBool }} {{ noiseInt 0.5 100 }} {{ noiseFloat 0.5 2 100.0 }} `+
		`{{ randomDate (dateModify "-24h" now) now }} {{ noiseDatePgInterval "1 day" now }} `+
		`{{ tsModify "1 day" now }}`)
	require.NoError(t, err)

	results := make([][]string, numGoroutines)
	errs := make([]error, numGoroutines)

	var wg sync.WaitGroup
	for g := range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range numIterations {
				buf := &strings.Builder{}
				if err := tmpl.Execute(buf, nil); err != nil {
					errs[g] = err
					return
				}
				results[g] = append(results[g], buf.String())
			}
		}()
	}
	wg.Wait()

	for g := range numGoroutines {
		require.NoError(t, errs[g])
		require.Len(t, results[g], numIterations)
		for _, res := range results[g] {
			// a corrupted rng can also return values outside the requested
			// bounds instead of panicking, so check the first field
			randomInt, err := strconv.Atoi(strings.Fields(res)[0])
			require.NoError(t, err)
			require.GreaterOrEqual(t, randomInt, minValue)
			require.LessOrEqual(t, randomInt, maxValue)
		}
	}
}
