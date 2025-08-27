package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"net/http"
)

import _ "github.com/golang-jwt/jwt/v5"

// TODO - rename to loginHandler
// TODO - shorten the signature so loginDemo can exist in HandleFunc
func loginDemo(pgPool *pgxpool.Pool) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			ctx  = r.Context()
			rows pgx.Rows
		)
		// Check login credentials. If set and within database
		//
		_, _ = w.Write([]byte("{\"data\": \"resource data\"}\n"))

		// TODO - write this into an authorizer middleware 'basicAuthorizer' or similar
		// Read headers
		user, pass, ok := r.BasicAuth()
		if !ok {
			// HTTP 401 from middleware
			//w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("{\"error\": \"unauthorized\"}\n"))
		}
		// https://www.postgresql.org/docs/current/passwordcheck.html says to encrypt passwords before they're sent to Postgres
		// Try credentials against postgres outside middleware
		// See if user exists in postgres
		// Send (salted) password to postgres
		// Have postgres do a comparison and return a boolean if it matches
		var err error
		// https://www.postgresql.org/docs/current/queries-with.html
		// When do I use WITH versus AS?
		if rows, err = pgPool.Query(ctx,
			`WITH pw_stored AS (
					SELECT password FROM auth WHERE uuid = $1 LIMIT 1
				), SELECT pw_stored = $2`, user, pass); err == nil {
			//"WITH pw_stored AS (SELECT password FROM auth WHERE uuid = $1 LIMIT 1)", user); err == nil {
		}

		_ = rows
		_ = rows

		// Storing passwords in Postgres https://docs.vultr.com/how-to-securely-store-passwords-using-postgresql
		// Should salted passwords be generated in Postgres or Golang?
		// SELECT crypt(<password>, gen_salt('md5'));

		// Bearer token is probably better than Basic, but is easier than JWT
		// JWT because that seems interesting
		// Need to look at the authorization header
		// https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Authorization
	}
}

// Using basic auth for now because that's easy and the React frontend should be using TLS, so FE -> BE communication iff
// Located on the same machine shouldn't be an issue

//var jwtKey = []byte("my_secret_key")
//var tokens []string

//type Claims struct {
//	Username string `json:"username"`
//	jwt.RegisteredClaims
//}

func createUser(pgPool *pgxpool.Pool) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			//ctx          = r.Context()
			//rows         pgx.Rows
			b     []byte
			input createUserInput
		)
		// Note - r.Body.Read is NOT the correct method to get data back
		b, err := io.ReadAll(r.Body)
		if b == nil || err != nil {
			w.WriteHeader(http.StatusBadRequest)
		} else if err := json.Unmarshal(b, &input); err != nil || input.Username == *new(string) {
			/* *new(T) is a very convoluted way to avoid knowing the zero value of a type while referencing it.
			in this case, a comparison to a zero value struct would likely be the more appropriate solution since I
			always want to ensure there are no 0 values. See https://github.com/golang/go/issues/61372
			*/
			w.WriteHeader(http.StatusBadRequest)
		}
		username := input.Username

		// TODO - fix the case where username is an empty string

		uuuid := uuid.New()
		err = pgPool.QueryRow(context.Background(), "INSERT INTO users VALUES ($1, $2)",
			// TODO - do I save these queries as variables? Should validation be done at insert time or at query time?
			uuuid.String(),
			username,
		).Scan()
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				// TODO - fmt -> log
				fmt.Println(pgErr.Message)
				fmt.Println(pgErr.Code)
			}
		}

		_ = err

		res, err := json.Marshal(struct {
			User string `json:"user"`
			UUID string `json:"uuid"`
		}{
			User: username,
			UUID: uuuid.String(),
		})
		_, err = w.Write(res)
		if err != nil {
			fmt.Println(err)
		}
		// TODO - check if user name already exists case
	}
}

// Using basic auth for now because that's easy and the React frontend should be using TLS, so FE -> BE communication iff
// Located on the same machine shouldn't be an issue

//var jwtKey = []byte("my_secret_key")
//var tokens []string

//	type Claims struct {
//		Username string `json:"username"`
//		jwt.RegisteredClaims
//	}
type createUserInput struct {
	Username string `json:"username"`
}

type alterUserInput struct {
	Username  string    `json:"username"`
	UUID      uuid.UUID `json:"uuid"`
	Operation string    `json:"operation"`
	Password  string    `json:"password"`
}
