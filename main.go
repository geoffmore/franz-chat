package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/peterbourgon/ff/v3"
	_ "github.com/peterbourgon/ff/v3"
	"io"
	"log"
	"net/http"
	"strings"
)

const (
	chatTopic = "chat"
)

// type app struct {
// 	// updater
// }

// func (a *app) setupRoutes() {}
// func loginHandler {}
//
// func getMessagesHandler {}
//
// func postMessageHandler {}
//
// func createChannelHandler {}

func main() {
	var (
		kafkaConnection = flag.String("kafka.connection", "localhost:9094", "Kafka connection string")
		// kafkaConnection = flag.String("kafka.connection", "localhost:9092", "Kafka connection string")
		port = flag.Int("port", 8008, "Listen port")
	)

	// kafkaConfig := sarama.NewConfig()
	// kafkaClient := sarama.NewClient
	// Needs a named consumer group

	if err := ff.Parse(flag.CommandLine, nil); err != nil {
		log.Fatal(err)
	}

	kafkaClientConfig := sarama.NewConfig()
	kafkaClientConfig.Producer.Return.Successes = true // Necessary for SyncProducer

	// See https://github.com/IBM/sarama/blob/main/examples/http_server/http_server.go
	kafkaClient, err := sarama.NewClient(strings.Split(*kafkaConnection, ","), kafkaClientConfig)
	if err != nil {
		log.Fatal(err)
	}
	kafkaProducer, err := sarama.NewSyncProducerFromClient(kafkaClient)
	if err != nil {
		log.Fatal(err)
	}

	kafkaProducer.SendMessage(&sarama.ProducerMessage{
		Topic: "chat",
		Value: sarama.StringEncoder("hello"),
	})

	mux := http.NewServeMux()
	// How do I get extra stuff into the http handler interface and/or how do I get more stuff into a route?

	mux.HandleFunc("/chat", postChatMessageHandler)
	mux.HandleFunc("/chat1", bar(kafkaProducer))

	// JSON or request params?
	// What if I have a func DoSomething(ctx context.Context, arg Arg) http.Handler? So I can access variables with my handlers as needed?
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), mux))
}

type PostChatRequest struct {
	// How does backend capture expected json content with struct tags?
	// Maybe I unmarshal into an interface and ask the question "Does the json object have the fields I need? and validate those fields against some struct or something
	Channel string `json:"channel,omitempty"`
	Msg     string `json:"msg,omitempty"`
}

// Requires channel definition, which for now, can be arbitrary
func postChatMessageHandler(writer http.ResponseWriter, request *http.Request) {
	var (
		// ctx  = request.Context() // Why reference an existing context instead of creating a child context?
		body PostChatRequest
		b    []byte
	)
	if request.Method != http.MethodPost {
		log.Printf("invalid method")
		// Return invalid method
	}

	if request.Body == nil {
		// Return needs request body
	}
	// use ioutil.ReadAll
	_, err := request.Body.Read(b)
	// Maybe use content type detection algorithm to save some time
	if err != nil {
		log.Printf("unable to read body")
	}

	if err := json.Unmarshal(b, &body); err != nil {
		log.Printf("unable to unmarshal json")
	}
	// other headers must be set before a response header
	// writer.Header().Set("Content-Type", "application/json" or whatever constant lives in http)
	// writer.WriteHeader(http.StatusAccepted)
	// Use return to get out of the handler function if there are errors
	fmt.Printf("%+v", body)

	// Try json. Return http error code for content type application/json if it isn't json
	fmt.Println("/chat")
}

// func postKafkaMessage(message PostChatRequest)

// Create channel
// Post message to channel

// Maybe a signature that take a method, expecteded headers, struct for the expected fields, and a body with which to read

func foo(mux *http.ServeMux, path string, fn func(w http.ResponseWriter, r *http.Request), kafkaClient sarama.Client) {

	mux.HandleFunc(path, fn)
}

func bar(syncProducer sarama.SyncProducer) func(w http.ResponseWriter, r *http.Request) {

	syncProducer.SendMessage(&sarama.ProducerMessage{
		Topic: "chat",
		Value: sarama.StringEncoder("hello"),
	})

	// How can I do this better?
	return func(w http.ResponseWriter, r *http.Request) {
		var postChatRequest PostChatRequest
		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Println("unable to read body")
		}
		if err := json.Unmarshal(body, &postChatRequest); err != nil {
			log.Println("unable to unmarshal json body")
		}
		_, _, err = syncProducer.SendMessage(&sarama.ProducerMessage{
			Topic: "chat",
			Value: sarama.StringEncoder(postChatRequest.Msg),
		})
		log.Print(err)
	}
}
