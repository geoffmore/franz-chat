package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/geoffmore/franz-chat/internal/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
)

func main() {
	var (
		port               int
		kafkaConnection    string
		postgresConnection string
		serviceName        string
		serviceVersion     string
	)
	flag.IntVar(&port, "port", 8008, "Listen port")
	flag.StringVar(&kafkaConnection, "kafka.connection", "localhost:9092", "Kafka connection string")
	flag.StringVar(&postgresConnection, "postgres.connection", "postgresql://franz_chat:franz_chat@localhost:5432/franz_chat?application_name=franz_chat", "Postgres connection string")
	flag.StringVar(&serviceName, "service.name", "franz-chat", "Service name")
	flag.StringVar(&serviceVersion, "service.version", "v0.0.0", "Service version")

	if err := ff.Parse(flag.CommandLine, os.Args[1:], ff.WithEnvVars()); err != nil {
		log.Fatal(err)
	}

	// Init configs
	kafkaCfg := kafka.NewKafkaConfig(&kafkaConnection)

	// Init stateful connections
	asyncProducer := kafka.NewAsyncProducer(kafkaCfg) // TODO - there is a bug here
	pgConn, err := pgx.Connect(context.Background(), postgresConnection)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer func(pgConn *pgx.Conn, ctx context.Context) {
		err := pgConn.Close(ctx)
		if err != nil {
			fmt.Println(err)
		}
	}(pgConn, context.Background())

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
		err := pgConn.QueryRow(context.Background(), "INSERT INTO messages VALUES ($1, $2)", uuid.New().String(), strings.Join(message, ""))
		if err != nil {
			fmt.Println(err)
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
