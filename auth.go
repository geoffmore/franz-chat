package main

import (
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
)

// basicAuthMiddleware is deprecated in favor of hasBasicAuth
func basicAuthMiddleware(pgPool *pgxpool.Pool, h http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			var (
				ctx            = r.Context()
				user, pass, ok = r.BasicAuth()
			)
			// Not Authorization header case
			if !ok {
				// HTTP 401 from middleware
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte("{\"error\": \"unauthorized\"}\n"))
				return
			}
			// TODO - remove

			// https://www.postgresql.org/docs/current/passwordcheck.html says to encrypt passwords before they're sent to Postgres
			// Try credentials against postgres outside middleware
			// See if user exists in postgres
			// Send (salted) password to postgres
			// Have postgres do a comparison and return a boolean if it matches

			// User doesn't exist TODO
			// ???
			// Credentials don't match user TODO
			// return http.NotAuthorized

			var err error
			var rows pgx.Rows
			// https://www.postgresql.org/docs/current/queries-with.html
			// When do I use WITH versus AS?
			if user != "" && pass != "" {
				if rows, err = pgPool.Query(ctx,
					// TODO - figure out how to get a single entry here maybe without a named variable
					`WITH pw_stored AS (SELECT password FROM auth WHERE uuid = $1 LIMIT 1) SELECT pw_stored = $2;`, user, pass); err == nil {
					fmt.Println(err)
					//"WITH pw_stored AS (SELECT password FROM auth WHERE uuid = $1 LIMIT 1)", user); err == nil {
				}
			}
			_ = rows
			if err != nil {
				//
			}
			h.ServeHTTP(w, r) // Runs http.Handler h after this middleware
		},
	)
}

// withBasicAuth is deprecated
func withBasicAuth(pgPool *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	var (
		ctx = r.Context()
	)
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
	var rows pgx.Rows
	// https://www.postgresql.org/docs/current/queries-with.html
	// When do I use WITH versus AS?
	if user != "" && pass != "" {
		if rows, err = pgPool.Query(ctx,
			// TODO - figure out how to get a single entry here maybe without a named variable
			`WITH pw_stored AS (SELECT password FROM auth WHERE uuid = $1 LIMIT 1) SELECT pw_stored = $2;`, user, pass); err == nil {
			fmt.Println(err)
			//"WITH pw_stored AS (SELECT password FROM auth WHERE uuid = $1 LIMIT 1)", user); err == nil {
		}
	}
	_ = rows
	if err != nil {
		//
	}
}

// checkBasicAuth checks that basic auth credentials are valid within the database. This extends hasBasicAuth.
func checkBasicAuth(pgPool *pgxpool.Pool) http.Handler {
	// TODO - check with Go devs to see if I should make this middleware have postgres credentials. It would probably be ugly if so
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			var (
				ctx            = r.Context()
				user, pass, ok = r.BasicAuth()
				rows           pgx.Rows
				err            error
			)
			// Not Authorization header case
			if !ok || user == "" || pass == "" {
				w.WriteHeader(http.StatusUnauthorized)
				// TODO - use the encoder or make an error type
				_, _ = w.Write([]byte("{\"error\": \"unauthorized\"}\n"))
				return
			}
			// https://www.postgresql.org/docs/current/passwordcheck.html says to encrypt passwords before they're sent to Postgres
			// Try credentials against postgres outside middleware
			// See if user exists in postgres
			// Send (salted) password to postgres
			// Have postgres do a comparison and return a boolean if it matches

			// https://www.postgresql.org/docs/current/queries-with.html
			// When do I use WITH versus AS?
			if rows, err = pgPool.Query(ctx,
				// TODO - figure out how to get a single entry here maybe without a named variable
				`WITH pw_stored AS (SELECT password FROM auth WHERE uuid = $1 LIMIT 1) SELECT pw_stored = $2;`, user, pass); err == nil {
				fmt.Println(err)
				//"WITH pw_stored AS (SELECT password FROM auth WHERE uuid = $1 LIMIT 1)", user); err == nil {
			}
			_ = rows
			// TODO - handle output
			if err != nil {
			}
			// h.ServeHTTP(w, r) // Runs http.Handler h after this middleware
		},
	)
}

/* TODO - cleanup notes
	https://www.postgresql.org/docs/current/passwordcheck.html says to encrypt passwords before they're sent to Postgres
	Try credentials against postgres outside middleware
	See if user exists in postgres
	Send (salted) password to postgres
	Have postgres do a comparison and return a boolean if it matches
	https://www.postgresql.org/docs/current/queries-with.html
	When do I use WITH versus AS?
	`WITH pw_stored AS (
			SELECT password FROM auth WHERE uuid = $1 LIMIT 1
		), SELECT pw_stored = $2`, user, pass)`

 	Storing passwords in Postgres https://docs.vultr.com/how-to-securely-store-passwords-using-postgresql
 	Should salted passwords be generated in Postgres or Golang?
 	SELECT crypt(<password>, gen_salt('md5'));

 	Bearer token is probably better than Basic, but is easier than JWT
 	JWT because that seems interesting
 	Need to look at the authorization header
 	https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Authorization

	Using basic auth for now because that's easy and the React frontend should be using TLS, so FE -> BE communication iff
	 Located on the same machine shouldn't be an issue

	var jwtKey = []byte("my_secret_key")
	var tokens []string

	type Claims struct {
		Username string `json:"username"`
		jwt.RegisteredClaims
	}
*/
