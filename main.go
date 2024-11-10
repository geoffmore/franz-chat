package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/geoffmore/franz-chat/internal/kafka"
	"github.com/jackc/pgx/v5"

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
		err := pgPool.QueryRow(context.Background(), "INSERT INTO messages VALUES ($1, $2, $3)",
			uuid.New().String(),
			strings.Join(message, ""),
			start,
		).Scan()

		// TODO -r.
		// https://github.com/jackc/pgx/wiki/Error-Handling TODO - make a query/error handling function
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				fmt.Println(pgErr.Message)
				fmt.Println(pgErr.Code)
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
		//// TODO - template index.html to set this key programatically
		channel, ok := r.PostForm["channel"] // TODO - sanitize channel name
		if !ok {
			// Invalid key and/or blank form message
		}
		//asyncProducer.ProduceMessage(ctx, &sarama.ProducerMessage{Topic: chatTopic, Value: sarama.StringEncoder(strings.Join(message, ""))})
		//// Write to postgres
		//// Should I generate a UUID on message send (for listen/notify) or should I have postgres send back a uuid on commit?
		//// See https://github.com/jackc/pgx/wiki/Getting-started-with-pgx
		//// TODO - use a prepared statement
		//// TODO - See https://github.com/jackc/pgx/wiki/UUID-Support and maybe use uuid.New() instead of uuid.New().String()
		//// TODO - use pgxpool instead of pgx
		//// TODO - unable to send more than a single message in an app run
		//// TODO - use pgConn.SendBatch to send multiple messages
		//// NOTE - statements are prepared/cached automagically with pgx
		////
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

	//consumer := kafka.NewConsumer()
	//consumerCtx := context.WithCancel(context.Background())

	channelMessages := make(map[string][channelMessageLimit]ChatMessage)
	_ = channelMessages

	http.HandleFunc("/messages",
		func(w http.ResponseWriter, r *http.Request) {
			var (
				ctx = r.Context()
			)
			_ = ctx
			tmpl, err := template.ParseFiles("templates/chat-messages.html")
			if err != nil {
				log.Printf(err.Error())
			}
			err = tmpl.Execute(w, nil)
			if err != nil {
				log.Printf(err.Error())
			}
			// Get all messages from kafka
			// Render a template
			// Return template to htmx
		},
		// Only grab latest 100 messages from each channel (because of laziness)
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

// TODO - listen/notify

// create/join channel button that temporarily modifies the channel window

// TODO - restrict access to api endpoints to JUST htmx (possibly by headers)
