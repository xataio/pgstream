// SPDX-License-Identifier: Apache-2.0

package otel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

type Provider struct {
	meterProvider  metric.MeterProvider
	tracerProvider trace.TracerProvider
	shutdownFns    []func(context.Context) error
}

const serviceName = "pgstream"

func NewProvider(cfg *Config) (*Provider, error) {
	o := &Provider{}
	ctx := context.Background()
	if err := o.initMeterProvider(ctx, cfg.Metrics); err != nil {
		return nil, err
	}

	if err := o.initTracerProvider(ctx, cfg.Traces); err != nil {
		return nil, err
	}

	return o, nil
}

func (o *Provider) Meter(name string) metric.Meter {
	return o.meterProvider.Meter(name)
}

func (o *Provider) Tracer(name string) trace.Tracer {
	return o.tracerProvider.Tracer(name)
}

func (o *Provider) NewInstrumentation(name string) *Instrumentation {
	return &Instrumentation{
		Meter:  o.Meter(name),
		Tracer: o.Tracer(name),
	}
}

func (o *Provider) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, shutdownFn := range o.shutdownFns {
		if err := shutdownFn(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (o *Provider) initMeterProvider(ctx context.Context, metricsConfig *MetricsConfig) error {
	if metricsConfig == nil {
		o.meterProvider = metricnoop.NewMeterProvider()
		otel.SetMeterProvider(o.meterProvider)
		return nil
	}

	metricsExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithTemporalitySelector(deltaSelector),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithEndpoint(metricsConfig.Endpoint))
	if err != nil {
		return err
	}

	// periodic reader collects and exports metrics to the exporter at the
	// defined interval (defaults to 60s)
	reader := sdkmetric.NewPeriodicReader(metricsExporter, sdkmetric.WithInterval(metricsConfig.collectionInterval()))
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(newResource()),
		sdkmetric.WithReader(reader))
	o.shutdownFns = append(o.shutdownFns, mp.Shutdown)

	o.meterProvider = mp
	otel.SetMeterProvider(o.meterProvider)

	return nil
}

func (o *Provider) initTracerProvider(ctx context.Context, tracesConfig *TracesConfig) error {
	if tracesConfig == nil {
		o.tracerProvider = tracenoop.NewTracerProvider()
		otel.SetTracerProvider(o.tracerProvider)
		return nil
	}

	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(tracesConfig.Endpoint),
	)
	if err != nil {
		return err
	}

	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(tracesConfig.SampleRatio))
	// Register the trace exporter with a TracerProvider, using a batch
	// span processor to aggregate spans before export.
	batchSpanProcessor := sdktrace.NewBatchSpanProcessor(traceExporter)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(newResource()),
		sdktrace.WithSpanProcessor(batchSpanProcessor),
		sdktrace.WithSampler(sampler))
	o.shutdownFns = append(o.shutdownFns, tp.Shutdown)

	o.tracerProvider = tp
	otel.SetTracerProvider(o.tracerProvider)

	return nil
}

func newResource() *resource.Resource {
	return resource.NewSchemaless(
		semconv.ServiceNameKey.String(serviceName),
		semconv.ServiceVersionKey.String(version()),
	)
}

// OpenTelemetry protocol supports two ways of representing metrics in time:
// Cumulative and Delta temporality. This function sets the temporality
// preference of the OpenTelemetry implementation to DELTA, because setting it
// to CUMULATIVE may discard some data points during application (or collector)
// startup.
func deltaSelector(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	switch kind {
	case sdkmetric.InstrumentKindCounter,
		sdkmetric.InstrumentKindHistogram,
		sdkmetric.InstrumentKindObservableGauge,
		sdkmetric.InstrumentKindObservableCounter:
		return metricdata.DeltaTemporality
	case sdkmetric.InstrumentKindUpDownCounter,
		sdkmetric.InstrumentKindObservableUpDownCounter:
		return metricdata.CumulativeTemporality
	default:
		panic("unknown instrument kind")
	}
}
