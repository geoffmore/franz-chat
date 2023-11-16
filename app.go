package main

import (
	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"log"
	"time"
)

type app struct {
	postgresClient     *pgx.Conn
	kafkaSyncProducer  sarama.SyncProducer
	kafkaAsyncProducer sarama.AsyncProducer
	kafkaConsumerGroup sarama.ConsumerGroup
	pgPool             *pgxpool.Pool
	tracer             trace.Tracer
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
	traceExporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return err // nil, err
	}
	//traceExporter, err := otlptracegrpc.New(context.Background())
	// "Ensure default SDK resources and the required service name are set."
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			attribute.KeyValue{
				Key:   semconv.ServiceNameKey,
				Value: attribute.StringValue(name),
			},
		),
	)
	// TraceProvider
	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter, sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithResource(r),
	)

	// Set global trace provider
	otel.SetTracerProvider(traceProvider)
	a.tracer = traceProvider.Tracer("")
	return nil
}
