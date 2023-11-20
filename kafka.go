package main

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/dnwe/otelsarama"
	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"
	"log"
	"strings"
	"time"
)

// Wraps sarama.Config to have a struct capable of making a client w/ optional span tracing
type kafkaConfig struct {
	// Should the embedded sarama.Config be mutable? Yes because generating producers/consumers will need to modify config
	*sarama.Config
	conn   *string // Needs a conn to gen client without passing extra args
	tracer oteltrace.Tracer
}

type producerMetadata struct {
	tracer oteltrace.Tracer
}

// AsyncProducer wraps sarama.AsyncProducer with a tracer. Necessary because otelsarama.WrapAsyncProducer leaves nothing indicate that a producer has been wrapped
type AsyncProducer struct {
	*producerMetadata
	producer sarama.AsyncProducer
}

// SyncProducer wraps sarama.SyncProducer with a tracer. Necessary because otelsarama.WrapAsyncProducer leaves nothing indicate that a producer has been wrapped
type SyncProducer struct {
	*producerMetadata
	producer sarama.SyncProducer
}

// Creates the default sarama.Config with connection details for client gen
func newKafkaConfig(conn *string) *kafkaConfig {
	return &kafkaConfig{
		Config: sarama.NewConfig(),
		conn:   conn,
	}
}

// Creates a AsyncProducer with opinions and optional tracing
func newAsyncProducer(cfg *kafkaConfig) *AsyncProducer {
	// See https://github.com/IBM/sarama/blob/main/examples/http_server/http_server.go

	var p AsyncProducer
	var metadata producerMetadata

	// Async Settings
	cfg.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	cfg.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	cfg.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	c, err := sarama.NewClient(strings.Split(*cfg.conn, ","), cfg.Config)
	if err != nil {
		log.Fatal(err)
	}

	kafkaProducer, err := sarama.NewAsyncProducerFromClient(c)
	if err != nil {
		log.Fatal(err)
	}
	p.producer = kafkaProducer
	// https://github.com/IBM/sarama/issues/2510
	if cfg.tracer != nil {
		metadata.tracer = cfg.tracer
		p.producer = otelsarama.WrapAsyncProducer(c.Config(), p.producer)
	}

	p.producerMetadata = &metadata

	// TODO - use a logger for this instead of logging to stdout
	// TODO - make a function that handles logging errors from chan
	go func() {
		for err := range p.producer.Errors() {
			log.Println("Failed to write message to Kafka", err)
		}
	}()
	return &p
}

// Creates a SyncProducer with opinions and optional tracing
func newSyncProducer(cfg *kafkaConfig) *SyncProducer {
	// See https://github.com/IBM/sarama/blob/main/examples/http_server/http_server.go

	var p SyncProducer
	var metadata producerMetadata

	// Sync Settings
	cfg.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	cfg.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	cfg.Producer.Return.Successes = true

	c, err := sarama.NewClient(strings.Split(*cfg.conn, ","), cfg.Config)
	if err != nil {
		log.Fatal(err)
	}

	kafkaProducer, err := sarama.NewSyncProducerFromClient(c)
	if err != nil {
		log.Fatal(err)
	}
	p.producer = kafkaProducer
	// https://github.com/IBM/sarama/issues/2510
	if cfg.tracer != nil {
		metadata.tracer = cfg.tracer
		p.producer = otelsarama.WrapSyncProducer(c.Config(), p.producer)
	}

	p.producerMetadata = &metadata

	return &p
}

func (p *AsyncProducer) produceMessage(ctx context.Context, msg *sarama.ProducerMessage) {
	// https://github.com/dnwe/otelsarama/blob/main/example/producer/producer.go

	// TODO - determine how to determine if an AsyncProducer was modified with otelsarama.WrapAsyncProducer
	if p.tracer != nil {
		otel.GetTextMapPropagator().Inject(ctx, otelsarama.NewProducerMessageCarrier(msg))
	}
	p.producer.Input() <- msg
}

func (p *SyncProducer) produceMessage(ctx context.Context, msg *sarama.ProducerMessage) {
	if p.tracer != nil {
		otel.GetTextMapPropagator().Inject(ctx, otelsarama.NewProducerMessageCarrier(msg))
	}
	if _, _, err := p.producer.SendMessage(msg); err != nil {
		log.Println("Failed to write message to Kafka", err)
	}
}
