package events

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"os"
)

func Trace(ctx context.Context, name string) (func(context.Context) error, error) {
	opts := []otlptracehttp.Option{otlptracehttp.WithInsecure()}
	if endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); endpoint != "" {
		opts = append(opts, otlptracehttp.WithEndpointURL(endpoint+"/v1/traces"))
	}
	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, err
	}
	r, _ := resource.Merge(resource.Default(), resource.NewWithAttributes("", attribute.String("service.name", name)))
	p := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(r))
	otel.SetTracerProvider(p)
	return p.Shutdown, nil
}
