package main

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
)

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
