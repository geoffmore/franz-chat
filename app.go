package main

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"log"
)

type app struct {
	postgresClient     *pgx.Conn
	kafkaSyncProducer  sarama.SyncProducer
	kafkaAsyncProducer sarama.AsyncProducer
	kafkaConsumerGroup sarama.ConsumerGroup
	pgPool             *pgxpool.Pool
	tracer             trace.Tracer
	// name               string
	// version            string
}

func (a *app) Close() error {
	if err := a.kafkaAsyncProducer.Close(); err != nil {
		log.Println("Failed to close kafka AsyncProducer!")
	}
	return nil
}

func (a *app) RegisterTracer(name string) error {
	// https://opentelemetry.io/docs/instrumentation/go/getting-started/
	// OpenTelemetry init

	// TraceExporter
	// Defaults to https://localhost:4317
	// Expects env var OTEL_EXPORTER_OTLP_ENDPOINT
	exp, err := otlptracegrpc.New(context.Background())

	// traceExporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return err // nil, err
	}
	// "Ensure default SDK resources and the required service name are set."
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(name),
			semconv.ServiceVersion("0.0.1"),
		),
	)
	// TraceProvider
	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	)

	// Set global trace provider
	otel.SetTracerProvider(traceProvider)
	// Set W3C TextMap propogator
	otel.SetTextMapPropagator(propagation.TraceContext{})
	a.tracer = traceProvider.Tracer("")
	return nil
}

func newApp() *app {
	return &app{}
}
