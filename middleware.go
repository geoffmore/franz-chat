package main

import (
	"net/http"
)

// noOpMiddleware is a middleware that does nothing itself and instead returns the http.Handler input unmodified.
// This is useful to test middleware behaviour and as an exemplar/base for other middleware
// Originally, this had a signature func(http.Handler) http.Handler, but I needed a postgres connection pointer receiver, so
// I arrived at this as closure around a function that modifies http.Handler
func noOpMiddleware() func(handler http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				// Work goes here
				h.ServeHTTP(w, r)
			},
		)
	}
}

// checkContentType is a middleware function that compares the input string to the 'Content-Type' header in the request
func checkContentType(s string) func(handler http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Content-Type") != s {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				h.ServeHTTP(w, r)
			},
		)
	}
}

func hasBasicAuth() func(handler http.Handler) http.Handler {
	// TODO - check with Go devs to see if I should make this middleware have postgres credentials. It would probably be ugly and breaks rules if so
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				var (
					user, pass, ok = r.BasicAuth()
				)
				// Not Authorization header case
				if !ok || user == "" || pass == "" {
					w.WriteHeader(http.StatusUnauthorized)
					// TODO - use the encoder or make an error type
					_, _ = w.Write([]byte("{\"error\": \"unauthorized\"}\n"))
					return
				}
				h.ServeHTTP(w, r)
			},
		)
	}
}

// composeMiddleware takes a handler and a list of middleware and sequentially applies that middleware - returning an http.Handler.
// This is necessary to avoid having to add a middleware argument within handler function signatures and keep the surface area
// of those handlers relatively smaller.
// The middleware argument is complex, but I need the ability to use closures, take http.Handler, and return http.Handler
// And thus felt like the easiest way to do so
func composeMiddleware(
	handler http.Handler,
	middleware ...func(http.Handler) http.Handler) http.Handler {
	// TODO - check that apply order is preserved (1st argument should be applied first)
	for _, v := range middleware {
		handler = v(handler)
	}
	return handler
}
