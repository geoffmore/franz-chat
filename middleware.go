package main

import (
	"fmt"
	"net/http"
)

type Middleware func(handler Handler) Handler

func midContentType(s string) Middleware {
	return func(handler Handler) Handler {
		return func(r *http.Request) (int, any) {
			fmt.Println(s)
			return handler(r)
		}
	}
}

/*
midNoOp is a Middleware that returns the handler provided with no modifications. Useful to test Middleware functionality
and as a base for other Middleware
*/
func midNoOp() Middleware {
	return func(handler Handler) Handler {
		return func(r *http.Request) (int, any) {
			return handler(r)
		}
	}
}
