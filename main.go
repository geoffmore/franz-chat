package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/geoffmore/franz-chat/internal/lib"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"os/signal"

	// How do I support this uuid natively in PGX. Will this improve performance?
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/peterbourgon/ff/v3"
	"html/template"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// TODO - determine how to make this a non-package-scoped variable and maintain use in func bar
var (
	templateMessage *template.Template
)

const (
	chatTopic           = "chat"
	appName             = "franz-api"
	channelMessageLimit = 100
	defaultChannel      = "general"
	messagesTable       = "messages"

	defaultLineLimit                  = 20
	defaultClientChannelMessageBuffer = 20
)

func main() {
	var (
		port               int
		kafkaConnection    string
		postgresConnection string
		serviceName        string
		serviceVersion     string
		startupTimeout     time.Duration
		logLevel           string
	)
	// TODO - use a FlagSet instead of global flags
	flag.IntVar(&port, "port", 8008, "Listen port")
	flag.StringVar(&kafkaConnection, "kafka.connection", "localhost:9092", "Kafka connection string")
	flag.StringVar(&postgresConnection, "postgres.connection", "postgresql://franz_chat:franz_chat@localhost:5432/franz_chat?application_name=franz_chat", "Postgres connection string")
	flag.StringVar(&serviceName, "service.name", "franz-chat", "Service name")
	flag.StringVar(&serviceVersion, "service.version", "v0.0.0", "Service version")
	flag.DurationVar(&startupTimeout, "startup.timeout", 30*time.Second, "Startup timeout")
	flag.StringVar(&logLevel, "log.level", "info", "log level")

	if err := ff.Parse(flag.CommandLine, os.Args[1:], ff.WithEnvVars()); err != nil {
		// TODO - determine how to use logger here
		log.Fatal(err)
	}

	// TODO - move initialization to NewServer/NewApp/NewService. This should have <Foo>Stores

	// Initialize logger
	// TODO - define log schema
	logger := lib.NewLogger(serviceName, serviceVersion, logLevel)

	// Load templates
	var (
		templatePostChat    *template.Template
		templateGetMessages *template.Template
	)
	templatePostChat = handleTemplateRender(logger, "./templates/postChat.html.tmpl")
	templateGetMessages = handleTemplateRender(logger, "./templates/getMessages.html.tmpl")
	templateMessage = handleTemplateRender(logger, "./templates/message.html.tmpl")

	// TODO - render index.html with initial template content. This should limit config drift

	// TODO - is it safe to assume that every endpoint returns an HTML object?

	// Init configs

	// Initialize stateful connections

	// TODO - try https://github.com/jackc/pgx/wiki/UUID-Support eventually with google/uuid

	pgPool, err := pgxpool.New(context.Background(), postgresConnection)
	if err != nil {
		logger.Fatal("", err)
	}
	defer pgPool.Close()

	c, err := pgPool.Acquire(context.Background())
	if err != nil {
	}

	// TODO - log info attempting to create default channel <defaultChannel value>. And another message for create/no-op/err
	// TODO - optimize this into a subquery
	// Create default channel on startup if not exists; grab its UUID
	// Maybe isDefault column would be helpful on the channels table
	// defaultChannelUUID =
	err = pgPool.QueryRow(context.Background(), "SELECT uuid FROM channels WHERE name == $1", defaultChannel).Scan()
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		// TODO - make default channel uuid all zeroes
		_ = pgPool.QueryRow(context.Background(), "INSERT INTO channels VALUES ($1, $2)", uuid.New(), defaultChannel)
	}

	// TODO - listen/notify
	// Setup Postgres Listener
	//_, err = pgPool.Exec(context.Background(), "LISTEN messages")
	//if err != nil {
	//	log.Fatal(err)
	//}

	// Create a Prometheus registry.
	registry := prometheus.NewRegistry()

	// Register default app metrics
	appMetrics := lib.NewAppMetrics(registry)

	// Acquire connection for metrics collector
	foo, err := pgPool.Acquire(context.Background())
	if err != nil {
		logger.Fatal("", err)
	}

	// Always initialize prometheus.Collector, which does not collect metrics until registered
	franzDBCollector := lib.NewFranzDBStatsCollector(context.Background(), foo)

	// Session lock based metrics registration
	go func() {
		for {
			lib.HandleSessionMetrics(context.Background(), c, logger, registry, franzDBCollector)
			time.Sleep(10 * time.Second)
		}
	}()

	// Init listen/notify
	// See https://github.com/jackc/pgx/blob/master/examples/chat/main.go
	notificationConn, err := pgPool.Acquire(context.Background())
	if err != nil {
		logger.Fatal("", err)
	}
	go lib.Listen(context.Background(), messagesTable, notificationConn)

	http.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		Registry: registry,
		// ErrorLog: logger, // TODO - add logger here
	}))
	// TODO - add a proper mux instead of using the default
	// Routes added via routes.go
	addRoutes(pgPool, http.DefaultServeMux)

	// Serve static assets
	// https://stackoverflow.com/questions/26559557
	// TODO - make sure /static/index.html is distinct from assets
	// TODO - make index.html into a template and make it composable with its children to prevent runtime modifications
	// TODO - convert this into a HandlerFunc that correlates all files with the original request

	// https://technology.blog.gov.uk/2013/12/05/building-a-new-router-for-gov-uk/ says I can use https://pkg.go.dev/net/http/httputil#NewSingleHostReverseProxy to rewrite URLs
	http.Handle("/static/",
		http.StripPrefix("/static/", http.FileServer(http.Dir("./html"))),
	)

	http.HandleFunc("/polling", bar)

	http.HandleFunc("/chat", func(w http.ResponseWriter, r *http.Request) {
		var (
			ctx = r.Context()
			// NOTE - http.Request doesn't have a direct request timestamp
			start = time.Now() // Note - https://stackoverflow.com/questions/15827329/
		)
		if err := r.ParseForm(); err != nil {
			logger.Error("", err)
		}
		// TODO - template index.html to set this key programatically
		message, ok := r.PostForm["message"]
		if !ok {
			// Invalid key and/or blank form message
		}
		// Write to postgres
		// Should I generate a UUID on message send (for listen/notify) or should I have postgres send back a uuid on commit?
		// See https://github.com/jackc/pgx/wiki/Getting-started-with-pgx
		// TODO - use a prepared statement
		// TODO - See https://github.com/jackc/pgx/wiki/UUID-Support and maybe use uuid.New() instead of uuid.New().String()
		// TODO - use pgxpool instead of pgx
		// TODO - unable to send more than a single message in an app run
		// TODO - use pgConn.SendBatch to send multiple messages
		// NOTE - statements are prepared/cached automagically with pgx

		msg := strings.Join(message, "")
		// Prevent empty messages from entering DB
		if len(msg) <= 0 {
			w.WriteHeader(http.StatusBadRequest) // Send 400 for htmx to handle button preservation
			logger.Warn("Unable to add empty message")
		} else {
			// TODO - exit early here rather than wrapping everything in this else statement
			messageUUID := uuid.New()

			// Insert message, then notify
			if err := pgPool.QueryRow(ctx, "INSERT INTO messages VALUES ($1, $2, $3);",
				messageUUID.String(),
				msg,
				start,
			).Scan(); lib.HandlePGError(err, pgx.ErrNoRows) != nil {
				logger.Error("", err)
			}

			// See https://www.postgresql.org/docs/current/sql-notify.html
			// Notify listeners. This statement cannot be in the same prepared statement according to SQLSTATE 42601.
			//if err := pgPool.QueryRow(ctx, "SELECT pg_notify('channels', $1);", messageUUID.String()).Scan(); lib.HandlePGError(err, pgx.ErrNoRows) != nil {
			//if err := pgPool.QueryRow(ctx, "SELECT pg_notify('channels', 'composeMiddleware');").Scan(); lib.HandlePGError(err, pgx.ErrNoRows) != nil {
			// Note - Query expects multiple rows, QueryRow expects 1 row, Exec expects no rows

			//if _, err := pgPool.Exec(context.Background(), "pg_notify($1, $2)", messagesTable, messageUUID.String()); err != nil {
			//	// Do work
			//}

			// TODO - add real values for channel id and channel name once they exist. Maybe just use channel id and rely on a lookup
			// TODO - debug metrics generation
			appMetrics.MessagesSentTotal.With(prometheus.Labels{lib.LabelChannelID: "TODO", lib.LabelChannelName: "TODO"}).Inc()
			err = templatePostChat.Execute(w, nil)
			if err != nil {
				logger.Error("", err)
			}
		}
	})

	// GET channels
	http.HandleFunc("/channels", func(w http.ResponseWriter, r *http.Request) {
		var (
			ctx  = r.Context()
			rows pgx.Rows
		)

		if rows, err = pgPool.Query(ctx, "SELECT name FROM channels"); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			fmt.Println(err)
		}

		/* NOTE - Rows must be closed to free up the connection according to https://pkg.go.dev/github.com/jackc/pgx/v5@v5.7.1#Rows
		but pgx.CollectRows does the collection automatically so Rows.Next() and Rows.Close() don't need to be called.
		*/
		// TODO - convert this to helper function to collect strings (if it isn't too complicated)
		channels, err := pgx.CollectRows[string](rows, func(row pgx.CollectableRow) (string, error) {
			var s string
			err := row.Scan(&s)
			return s, err
		})
		if err != nil {
			fmt.Println(err)
		}
		// Although ["composeMiddleware"] is valid json, React Promise.json() isn't happy with it, so I wrap the json in a struct here
		//data, err := json.Marshal(getChannelsResponse{
		//	Channels: channels,
		//})
		if err != nil {
			fmt.Println(err)
		}
		// TODO - wire this up with HTMX
		//if _, err := w.Write(data); err != nil {
		//	fmt.Println(err)
		//}
		w.Header().Set("Content-Type", "application/json")
		// These two access control headers are necessary when making calls to localhost
		// TODO - wrap these headers in an env-specific init thing
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.WriteHeader(http.StatusCreated)
		err = json.NewEncoder(w).Encode(GetMessagesResponse{Channels: channels})
		if err != nil {
			fmt.Println(err)
		}
		// TODO - figure out what each json handler needs and collect that into a set of common logic
	})

	http.HandleFunc("/create-channel", func(w http.ResponseWriter, r *http.Request) {
		// TODO - do I have to parse forms with HTMX each time?
		var (
			ctx = r.Context()
		)
		if err := r.ParseForm(); err != nil {
			fmt.Println(err)
		}
		_ = ctx
		// TODO - template index.html to set this key programatically
		channel, ok := r.PostForm["channel"] // TODO - sanitize channel name
		if !ok {
			// Invalid key and/or blank form message
		}
		// Write to postgres
		// Should I generate a UUID on message send (for listen/notify) or should I have postgres send back a uuid on commit?
		// See https://github.com/jackc/pgx/wiki/Getting-started-with-pgx
		// TODO - See https://github.com/jackc/pgx/wiki/UUID-Support and maybe use uuid.New() instead of uuid.New().String()
		// TODO - use pgConn.SendBatch to send multiple messages
		// NOTE - statements are prepared/cached automagically with pgx, but manual statements could be useful for VCS
		err := pgPool.QueryRow(context.Background(), "INSERT INTO channels VALUES ($1, $2)",
			uuid.New().String(),
			channel,
		).Scan()
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				// TODO - fmt -> log
				fmt.Println(pgErr.Message)
				fmt.Println(pgErr.Code)
			}
		}
	})

	type message struct {
		Timestamp time.Time // pgtype.Timestamp could be used, but it's more noisy
		Name      string
		Message   string
	}
	http.HandleFunc("/messages",
		func(w http.ResponseWriter, r *http.Request) {
			var (
				ctx  = r.Context()
				rows pgx.Rows
			)

			/* TODO - store latest timestamp in client, so when GET /messages is called, there is no need to get ALL messages
			For now, LIMIT is being used to offset the load, but OFFSET should be used eventually
			*/
			if rows, err = pgPool.Query(ctx, "SELECT message, date_trunc('minute', timestamp) FROM messages ORDER BY timestamp DESC LIMIT $1", defaultLineLimit); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				fmt.Println(err)
			}

			messages, err := pgx.CollectRows[message](rows, func(row pgx.CollectableRow) (message, error) {
				var msg message
				var pgTime pgtype.Timestamp
				err := row.Scan(&msg.Message, &pgTime) // https://github.com/jackc/pgx/issues/985#issuecomment-817026395
				msg.Timestamp = pgTime.Time
				msg.Name = "TODO" // TODO - this will come with user login. Perhaps there will be an anonymous user
				return msg, err
			})
			if err != nil {
				fmt.Println(err)
			}
			err = templateGetMessages.Execute(w, messages)
			if err != nil {
				fmt.Println(err)
			}
			/* TODO - maybe have the frontend send the uuid of the last message and some time comparison can be done to
			get all messages since then
			*/
		},
	)
	http.HandleFunc("/resource", loginDemo(pgPool))
	// POST /create-user
	http.HandleFunc("/create-user", createUser(pgPool))

	err = http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		// TODO - panic here

		logger.Error("", err) // TODO - wrap this call. Maybe avoid having to leak implementation
		os.Exit(1)
	}

	// TODO - investigate whether it's worth making assets reachable via index.html directly (probably not)
}

// Clients should always send html

// Fix button
// Then, add a channel option
// Then, add an option to add channels if they don't exist and/or create them

// Maybe try https://htmx.org/docs/#load_polling

// create/join channel button that temporarily modifies the channel window

// TODO - restrict access to api endpoints to JUST htmx (possibly by headers)

// https://github.com/jackc/pgx/blob/master/examples/chat/main.go

// handleTemplateRender is similar to template.Must(), but uses a logger before panic
func handleTemplateRender(logger *lib.Logger, filenames ...string) *template.Template {
	var (
		tmpl *template.Template
		err  error
	)
	if tmpl, err = template.ParseFiles(filenames...); err != nil {
		logger.Panic("Unable to render template!", err)
	}
	return tmpl
}

// TODO - clear chat on click
// TODO - onload, get channels for default channel and get the latest messages

func bar(w http.ResponseWriter, r *http.Request) {
	// See https://medium.com/@rian.eka.cahya/server-sent-event-sse-with-go-10592d9c2aa1
	//w.Header().Set("Access-Control-Allow-Origin", "*")
	//w.Header().Set("Access-Control-Expose-Headers", "Content-Type")

	//// w.Header().Set("Content-Type", "text/event-stream")
	//w.Header().Set("Cache-Control", "no-cache")
	//w.Header().Set("Connection", "keep-alive")

	// Simulate sending events (you can replace this with real data)
	err := templateMessage.Execute(w, nil)
	if err != nil {
		fmt.Println(err)
	}

	//for i := 0; i < 10; i++ {
	//	time.Sleep(1 * time.Second)
	//	// w.(http.Flusher).Flush()
	//}

	// Simulate closing the connection
	// TODO - replace with request context deadline or something
	//closeNotify := w.(http.CloseNotifier).CloseNotify()
	//<-closeNotify
	//_, _ = w, r
	//for {

	//}
	//// TODO - use channels instead
	//select {}
	//// close
}

// https://htmx.org/examples/update-other-content/

// See https://mholt.github.io/json-to-go/ for json -> Go conversion

type GetMessagesResponse struct {
	Channels []string `json:"channels"`
}

// Maybe every handler should be wrapped in such a way that it has a know reqeust and response?

/* Get channels

Get a list of channels
Return them as channel id: name pairs
Have React work with a scroll bar to show the list of channels. Maybe a diff can be used to avoid sending duplicates
	Maybe that diff function could be interesting
	Or maybe that is too much engineering for now
*/

func run(ctx context.Context, w io.Writer, args []string) error {

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()
	// ...
	return nil // Stub
}

/*
func main() {
	ctx := context.Background()
	if err := run(ctx, os.Stdout, os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.exit(1)
	}
}
*/

// TODO - shorten the signature so loginDemo can exist in HandleFunc
func loginDemo(pgPool *pgxpool.Pool) func(http.ResponseWriter, *http.Request) {
	checkBasicAuth(pgPool) // TODO - refactor
	return func(w http.ResponseWriter, r *http.Request) {
		// TODO - write this into an authorizer middleware 'basicAuthorizer' or similar
		// Read headers
		withBasicAuth(pgPool, w, r)

		// TODO - only return the rest-api if there isn't an error
		_, _ = w.Write([]byte("{\"data\": \"resource data\"}\n"))

	}
}
