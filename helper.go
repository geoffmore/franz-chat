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

func newKafkaClient(kafkaConnection *string) sarama.Client {
	kafkaClientConfig := sarama.NewConfig()
	kafkaClientConfig.Producer.Return.Successes = true // Necessary for SyncProducer

	// See https://github.com/IBM/sarama/blob/main/examples/http_server/http_server.go
	kafkaClient, err := sarama.NewClient(strings.Split(*kafkaConnection, ","), kafkaClientConfig)
	if err != nil {
		log.Fatal(err)
	}
	return kafkaClient
}

func newKafkaSyncProducer(c sarama.Client) sarama.SyncProducer {
	producer, err := sarama.NewSyncProducerFromClient(c)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	return producer
}

func newKafkaAsyncProducer(c sarama.Client) sarama.AsyncProducer {

	producer, err := sarama.NewAsyncProducerFromClient(c)
	if err != nil {
		log.Fatal(err)
	}
	// TODO - lock this behind a feature flag
	// https://github.com/IBM/sarama/issues/2510:w
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
