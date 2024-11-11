package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/geoffmore/franz-chat/internal/kafka"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	// How do I support this uuid natively in PGX. Will this improve performance?
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/peterbourgon/ff/v3"
	"html/template"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	chatTopic           = "chat"
	appName             = "franz-api"
	channelMessageLimit = 100
	defaultChannel      = "general"

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
	)
	flag.IntVar(&port, "port", 8008, "Listen port")
	flag.StringVar(&kafkaConnection, "kafka.connection", "localhost:9092", "Kafka connection string")
	flag.StringVar(&postgresConnection, "postgres.connection", "postgresql://franz_chat:franz_chat@localhost:5432/franz_chat?application_name=franz_chat", "Postgres connection string")
	flag.StringVar(&serviceName, "service.name", "franz-chat", "Service name")
	flag.StringVar(&serviceVersion, "service.version", "v0.0.0", "Service version")
	flag.DurationVar(&startupTimeout, "startup.timeout", 30*time.Second, "Startup timeout")

	if err := ff.Parse(flag.CommandLine, os.Args[1:], ff.WithEnvVars()); err != nil {
		log.Fatal(err)
	}

	// Load templates
	// TODO - instead of panic, gracefully exit with logger
	templatePostChat := template.Must(template.ParseFiles("./templates/postChat.html.tmpl"))
	templateGetMessages := template.Must(template.ParseFiles("./templates/getMessages.html.tmpl"))

	// TODO - render index.html with initial template content. This should limit config drift

	// TODO - is it safe to assume that every endpoint returns an HTML object?

	// Init configs
	kafkaCfg := kafka.NewKafkaConfig(&kafkaConnection)

	// Init stateful connections
	asyncProducer := kafka.NewAsyncProducer(kafkaCfg) // TODO - there is a bug here

	// TODO - try https://github.com/jackc/pgx/wiki/UUID-Support eventually with google/uuid

	pgPool, err := pgxpool.New(context.Background(), postgresConnection)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer pgPool.Close()

	// Create default channel on startup if not exists; grab its UUID
	// defaultChannelUUID =

	// TODO - log info attempting to create default channel <defaultChannel value>. And another message for create/no-op/err
	// TODO - optimize this into a subquery
	err = pgPool.QueryRow(context.Background(), "SELECT uuid FROM channels WHERE name == $1", defaultChannel).Scan()
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		// TODO - make default channel uuid all zeroes
		_ = pgPool.QueryRow(context.Background(), "INSERT INTO channels VALUES ($1, $2)", uuid.New(), defaultChannel)
	}

	// TODO - define log schema
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// TODO - listen/notify
	// Setup Postgres Listener
	//_, err = pgPool.Exec(context.Background(), "LISTEN messages")
	//if err != nil {
	//	log.Fatal(err)
	//}

	// Serve static assets
	// https://stackoverflow.com/questions/26559557
	// TODO - make sure /static/index.html is distinct from assets
	// TODO - convert this into a HandlerFunc that correlates all files with the original request
	http.Handle("/static/",
		http.StripPrefix("/static/", http.FileServer(http.Dir("./html"))),
	)

	http.HandleFunc("/chat", func(w http.ResponseWriter, r *http.Request) {
		var (
			ctx = r.Context()
			// NOTE - http.Request doesn't have a direct request timestamp
			start = time.Now() // Note - https://stackoverflow.com/questions/15827329/
		)
		if err := r.ParseForm(); err != nil {
			fmt.Println(err)
		}
		// TODO - template index.html to set this key programatically
		message, ok := r.PostForm["message"]
		if !ok {
			// Invalid key and/or blank form message
		}
		asyncProducer.ProduceMessage(ctx, &sarama.ProducerMessage{Topic: chatTopic, Value: sarama.StringEncoder(strings.Join(message, ""))})
		// Write to postgres
		// Should I generate a UUID on message send (for listen/notify) or should I have postgres send back a uuid on commit?
		// See https://github.com/jackc/pgx/wiki/Getting-started-with-pgx
		// TODO - use a prepared statement
		// TODO - See https://github.com/jackc/pgx/wiki/UUID-Support and maybe use uuid.New() instead of uuid.New().String()
		// TODO - use pgxpool instead of pgx
		// TODO - unable to send more than a single message in an app run
		// TODO - use pgConn.SendBatch to send multiple messages
		// NOTE - statements are prepared/cached automagically with pgx
		//

		msg := strings.Join(message, "")
		// Prevent empty messages from entering DB
		if len(msg) > 0 {
			err := pgPool.QueryRow(context.Background(), "INSERT INTO messages VALUES ($1, $2, $3)",
				uuid.New().String(),
				msg,
				start,
			).Scan()

			// https://github.com/jackc/pgx/wiki/Error-Handling TODO - make a query/error handling function
			if err != nil {
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) {
					fmt.Println(pgErr.Message)
					fmt.Println(pgErr.Code)
				}
			}
			err = templatePostChat.Execute(w, nil)
			if err != nil {
				fmt.Println(err)
			}
		} else {
			w.WriteHeader(http.StatusBadRequest) // Send 400 for htmx to handle button preservation
			fmt.Println("Unable to add empty message")
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
		// TODO - wire this up with HTMX
		if _, err := w.Write([]byte(strings.Join(channels, `\n`))); err != nil {
			fmt.Println(err)
		}

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

	channelMessages := make(map[string][channelMessageLimit]ChatMessage)
	_ = channelMessages

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

	err = http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		// TODO - panic here
		logger.Error(err.Error())
		os.Exit(1)
	}

	// TODO - investigate whether it's worth making assets reachable via index.html directly (probably not)
}

// Clients should always send html

type ChatMessage struct {
	Timestamp time.Time
	User      string
	Message   string
}

// Fix button
// Then, add a channel option
// Then, add an option to add channels if they don't exist and/or create them

// Maybe try https://htmx.org/docs/#load_polling

// create/join channel button that temporarily modifies the channel window

// TODO - restrict access to api endpoints to JUST htmx (possibly by headers)
