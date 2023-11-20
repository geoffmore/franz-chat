package main

import (
	"fmt"
)

type Middleware func(handler Handler) Handler

// What if this was a closure that returned a fn(Handler) Handler

func midContentType(s string) Middleware {
	return func(handler Handler) Handler {
		fmt.Println(s)
		return handler
	}
}

/*
midNoOp is a Middleware that returns the handler provided with no modifications. Useful to test Middleware functionality
and as a base for other Middleware
*/
func midNoOp() Middleware {
	return func(handler Handler) Handler {
		return handler
	}
}
