package main

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
)

func addRoutes(
	// Stores, logger, etc. that the app needs to run goes here
	pgPool *pgxpool.Pool,
	mux *http.ServeMux,
) {
	// NOTE - Handler funcs should return http.Handler types

	/*
		CREATE	- SQL INSERT
		READ	- SQL SELECT
		UPDATE	- SQL UPDATE
		DELETE	- SQL DELETE
	*/

	// TODO - add pgPool to all necessary handlers

	// Channel Operations
	//mux.Handle("/api/create-channel", createChannelHandler())
	mux.Handle("/api/create-channel",
		composeMiddleware(
			createChannelHandler(),
			checkContentType("application/json"),
			hasBasicAuth(),
		),
	)
	/*
		mux.Handle("/api/update-channel", updateChannelHandler())
		mux.Handle("/api/get-channels", getChannelsHandler())
		mux.Handle("/api/delete-channel", deleteChannelHandler())
		// User Operations
		mux.Handle("/api/create-user", createUserHandler())
		mux.Handle("/api/update-user", updateUserHandler())
		mux.Handle("/api/get-users", getUsersHandler())
		mux.Handle("/api/delete-user", deleteUserHandler())
		// Message Operations
		mux.Handle("/api/create-message", createMessageHandler())
		mux.Handle("/api/update-message", updateMessageHandler())
		mux.Handle("/api/get-messages", getMessagesHandler())
		mux.Handle("/api/delete-message", deleteMessageHandler())
		// Other stuff
		mux.Handle("/api/login", loginHandler())

	*/
}
