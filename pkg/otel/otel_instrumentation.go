// SPDX-License-Identifier: Apache-2.0

package otel

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type InstrumentationProvider interface {
	NewInstrumentation(name string) *Instrumentation
	Close() error
}

type Instrumentation struct {
	Meter  metric.Meter
	Tracer trace.Tracer
}

func (i *Instrumentation) IsEnabled() bool {
	return i != nil && (i.Meter != nil || i.Tracer != nil)
}

type noopProvider struct{}

func (p *noopProvider) NewInstrumentation(name string) *Instrumentation {
	return nil
}

func (p *noopProvider) Close() error {
	return nil
}

func NewInstrumentationProvider(cfg *Config) (InstrumentationProvider, error) {
	// if neither metrics or traces are configured, instrumentation is not
	// enabled. Return a noop with disabled instrumentation
	if cfg.Metrics == nil && cfg.Traces == nil {
		return &noopProvider{}, nil
	}
	return NewProvider(cfg)
}
