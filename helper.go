package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"log"
	"net/http"
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

type OkResponse struct {
	Msg string `json:"msg"`
}

func httpJSONOk() (int, []byte) {
	b, err := json.Marshal(
		OkResponse{
			Msg: "ok",
		},
	)
	if err != nil {
		log.Fatal("Unable to Marshal OkResponse into bytes!")
	}
	fmt.Println(http.DetectContentType(b))
	return http.StatusOK, b

}

func unmarshalInto(readCloser io.ReadCloser, v any) error {
	body, err := io.ReadAll(readCloser)
	if err != nil {
		log.Print(err)
		return err
	}
	if err = json.Unmarshal(body, v); err != nil {
		log.Println(err)
		return err
	}
	return err
}
