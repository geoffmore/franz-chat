package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/geoffmore/franz-chat/internal/kafka"
	"html/template"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	chatTopic = "chat"
	appName   = "franz-api"
)

func main() {
	var (
		port            = flag.Int("port", 8008, "Listen port")
		kafkaConnection = flag.String("kafka.connection", "localhost:9094", "Kafka connection string")
	)
	flag.Parse()

	// Init configs
	kafkaCfg := kafka.NewKafkaConfig(kafkaConnection)

	// Init stateful connections
	asyncProducer := kafka.NewAsyncProducer(kafkaCfg)

	// TODO - define log schema
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Serve static assets
	// https://stackoverflow.com/questions/26559557
	// TODO - make sure /static/index.html is distinct from assets
	// TODO - convert this into a HandlerFunc that correlates all files with the original request
	http.Handle("/static/",
		http.StripPrefix("/static/", http.FileServer(http.Dir("./html"))),
	)
	//http.HandleFunc("/", clientIndex)
	//http.HandleFunc("/src/htmx.min.js", foo)
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
	})
	http.HandleFunc("/test", testClientHandler)
	err := http.ListenAndServe(fmt.Sprintf(":%d", *port), nil)
	if err != nil {
		// TODO - panic here
		logger.Error(err.Error())
		os.Exit(1)
	}

	// TODO - investigate whether it's worth making assets reachable via index.html directly (probably not)
}

// Clients should always send html

func testClientHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("%+v\n", r)
	var b []byte
	if _, err := r.Body.Read(b); err != nil {
		fmt.Println(err)
	}
	if err := r.ParseForm(); err != nil {
		fmt.Println(err)
	}
	// TODO - template index.html to set this key programatically
	message, ok := r.PostForm["message"]
	if !ok {
		// Invalid key and/or blank form message
	}
	fmt.Println(message)
}

func foo(w http.ResponseWriter, r *http.Request) {
	var b []byte
	var err error
	if b, err = os.ReadFile("html/src/htmx.min.js"); err != nil {
		log.Fatal(err)
	}
	reader := bytes.NewReader(b)

	// Iterate over html directory
	// Generate a map of
	http.ServeContent(w, r, "foo", time.Now(), reader)
}

func clientIndex(w http.ResponseWriter, r *http.Request) {
	// TODO - send htmx with correct mime type
	// TODO - send css with correct mime type
	t, err := template.ParseFiles("html/index.html")
	if err != nil {
		log.Fatal(err)
	}
	err = t.Execute(w, nil)
	// I'm guessing a client request is typically accompanied by a server request
}
