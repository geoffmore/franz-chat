package main

import (
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
)

func newPostgresClient(postgresConnection *string) *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), *postgresConnection)
	if err != nil {
		log.Fatal("Unable to connect to database")
	}
	defer conn.Close(context.Background())
	return conn
}

func newPostgresConnPool(conn *string) *pgxpool.Pool {
	// https://stackoverflow.com/questions/70763022
	// Conn pool is needed for concurrency
	pool, err := pgxpool.New(context.Background(), *conn)
	if err != nil {
		log.Fatal("Unable to establish connection pool")
	}
	if err := pool.Ping(context.Background()); err != nil {
		log.Fatal("Unable to connect to DB using connection pool")
	}
	return pool
}

func newUUID() uuid.UUID {
	return uuid.New()
}
