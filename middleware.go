package main

type Middleware func(handler Handler) Handler

// What if this was a closure that returned a fn(Handler) Handler

/*
func midContentTypeJSON(s string) Middleware {
	// TODO
	return func(handler Handler) Handler {
	}
}
*/
