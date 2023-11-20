package main

import (
	"fmt"
	"github.com/julienschmidt/httprouter"
	oteltrace "go.opentelemetry.io/otel/trace"
	"log"
	"net/http"
)

// Contains server components
type service struct {
	// Based on perf bench at https://github.com/julienschmidt/go-http-routing-benchmark
	router *httprouter.Router
	tracer oteltrace.Tracer
	// logger
	// ...
}

func newService() service {
	s := service{
		router: httprouter.New(),
	}
	return s
}

// TODO determine if/why having a http.HandleFunc isn't as good as working only with the request

type Handler func(r *http.Request) (statusCode int, response any)

func (s *service) Start(port *int) {
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), s.router))
}

func (s *service) POST(pattern string, handler Handler, middleware ...Middleware) {
	s.router.Handler(http.MethodPost, pattern, s.wrapHandler(pattern, handler, middleware...))
}

func (s *service) wrapHandler(pattern string, handler Handler, middleware ...Middleware) http.Handler {
	for _, m := range middleware {
		// Do func types need be addressed by pointers to be mutable?
		m(handler)
	}
	// This makes 0 sense, but it works
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			// Setting content-type isn't necessary A b/c w.Write(b) calls DetectContentType(b)
			// See https://pkg.go.dev/net/http#DetectContentType
			// TODO - figure out a way to coerce json mime type in Handler return value
			// Or better yet, set that from the handler/middleware
			// See https://stackoverflow.com/questions/30310046
			rc, res := handler(r)

			w.WriteHeader(rc)
			switch res.(type) {
			// Write res directly if its concrete type is []byte
			case []byte:
				w.Write(res.([]byte))
			}
		},
	)
}

func (s *service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}
