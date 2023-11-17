package main

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/dnwe/otelsarama"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"strings"
	"time"
)

func newPostgresClient(postgresConnection *string) *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), *postgresConnection)
	if err != nil {
		log.Fatal("Unable to connect to database")
	}
	defer conn.Close(context.Background())
	return conn
}

func newPostgresConnPool(pgConn *string) *pgxpool.Pool {
	// https://stackoverflow.com/questions/70763022
	// Conn pool is needed for concurrency
	pool, err := pgxpool.New(context.Background(), *pgConn)
	if err != nil {
		log.Fatal("Unable to establish connection pool")
	}
	return pool
}

func newKafkaAsyncProducer(conn *string) sarama.AsyncProducer {
	// See https://github.com/IBM/sarama/blob/main/examples/http_server/http_server.go

	config := sarama.NewConfig()
	// Async Settings
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	c, err := sarama.NewClient(strings.Split(*conn, ","), config)
	if err != nil {
		log.Fatal(err)
	}

	producer, err := sarama.NewAsyncProducerFromClient(c)
	if err != nil {
		log.Fatal(err)
	}
	// TODO - lock this behind a feature flag
	// https://github.com/IBM/sarama/issues/2510
	if true {
		producer = otelsarama.WrapAsyncProducer(c.Config(), producer)
	}
	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write message to Kafka", err)
		}
	}()
	return producer
}

func newKafkaConsumerGroup(c sarama.Client, name string) sarama.ConsumerGroup {
	consumerGroup, err := sarama.NewConsumerGroupFromClient(name, c)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := consumerGroup.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	return consumerGroup
}

func newUUID() uuid.UUID {
	return uuid.New()
}
