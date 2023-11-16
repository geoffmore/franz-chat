package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v5"
	"github.com/peterbourgon/ff/v3"
	oteltrace "go.opentelemetry.io/otel/trace"
	"io"
	"log"
	"net/http"
)

const (
	chatTopic = "chat"
	appName   = "franz-api"
)

type FranzHandler interface {
	Handle(w http.ResponseWriter, r *http.Request)
}

// 'Needers' are a hopefully useful abstraction to automagically initialize structs with the respective client in app
type postgresNeeders interface{}
type kafkaNeeders interface{}

// kafkaSyncProducer sarama.SyncProducer
// postgresClient *pgx.Conn

type postgresUser struct {
	postgresClient *pgx.Conn
}

type getMessagesHandler struct {
	kafkaConsumer sarama.ConsumerGroup
}

func (h *getMessagesHandler) Handle(w http.ResponseWriter, r *http.Request) {

}

func main() {
	var (
		a                  app
		kafkaConnection    = flag.String("kafka.connection", "localhost:9094", "Kafka connection string")
		postgresConnection = flag.String("postgres.connection", "postgresql://franz_api:franz_api@localhost:5432/franz?application_name=franz_api", "Postgres connection")
		port               = flag.Int("port", 8008, "Listen port")
	)

	if err := ff.Parse(flag.CommandLine, nil); err != nil {
		log.Fatal(err)
	}

	if true { // TODO - wrap in trace enablement feature flag
		if err := a.RegisterTracer(appName); err != nil {
			log.Fatal(err)
		}
	}

	// Init stateful connections
	kafkaClient := newKafkaClient(kafkaConnection)
	a.kafkaSyncProducer = newKafkaSyncProducer(kafkaClient)
	a.kafkaAsyncProducer = newKafkaAsyncProducer(kafkaClient)
	a.kafkaConsumerGroup = newKafkaConsumerGroup(kafkaClient, appName)
	a.postgresClient = newPostgresClient(postgresConnection)
	a.pgPool = newPostgresConnPool(postgresConnection)

	defer func() {
		if err := (&a).Close(); err != nil {
			log.Println("failed to close server connections")
		}
	}()

	// Init http server
	mux := http.NewServeMux()
	// How do I get extra stuff into the http handler interface and/or how do I get more stuff into a route?

	// handleFunc is a replacement for mux.HandleFunc
	// which enriches the handler's HTTP instrumentation with the pattern as the http.route.
	/*
		handleFunc := func(pattern string, handlerFunc func(http.ResponseWriter, *http.Request)) {
			// Configure the "http.route" for the HTTP instrumentation.
			handler := otelhttp.WithRouteTag(pattern, http.HandlerFunc(handlerFunc))
			mux.Handle(pattern, handler)
		}

		_ = handleFunc
	*/
	/*
		handleFunc("/chat",
			(&postChatMessageHandler{kafkaSyncProducer: a.kafkaSyncProducer}).Handle,
		)
	*/

	mux.HandleFunc("/chat",
		// cannot call pointer method Handle on postChatMessageHandler
		(&postChatMessageHandler{
			producer: a.kafkaAsyncProducer,
			tracer:   a.tracer}).Handle,
	)
	mux.HandleFunc("/login",
		(&loginHandler{postgresClient: a.postgresClient}).Handle,
	)
	mux.HandleFunc("/create-channel",
		(&createChannelHandler{postgresClient: a.postgresClient}).Handle,
	)
	/*
		mux.HandleFunc("/create-user",
			createUserHandler{postgresClient: a.postgresClient}.Handle,
		)
	*/

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

type postChatMessageHandler struct {
	kafkaSyncProducer sarama.SyncProducer
	producer          sarama.AsyncProducer
	postgresClient    *pgx.Conn
	tracer            oteltrace.Tracer
}

func (h *postChatMessageHandler) Handle(w http.ResponseWriter, r *http.Request) {
	var postChatRequest PostChatRequest
	ctx, span := h.tracer.Start(r.Context(), "postChatMessageHandler")
	defer span.End()

	_ = ctx

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println("unable to read body")
	}
	if err := json.Unmarshal(body, &postChatRequest); err != nil {
		log.Println("unable to unmarshal json body")
	}
	// TODO - determine why only the first call to this handler produces a message in Kafka
	produceMessage(h.producer,
		&sarama.ProducerMessage{
			Topic: chatTopic,
			Value: sarama.StringEncoder(postChatRequest.Msg),
		},
	)
	if err != nil {
		log.Print(err)
	}
}

type loginHandler struct {
	postgresClient *pgx.Conn
}

func (h *loginHandler) Handle(w http.ResponseWriter, r *http.Request) {}

type createChannelHandler struct {
	postgresClient *pgx.Conn
}

func (h *createChannelHandler) Handle(w http.ResponseWriter, r *http.Request) {
	/*
		{
			"channel": "foo"
		}
	*/
	var nameThis struct {
		Channel string `json:"channel"`
	}

	if err := unmarshalInto(r.Body, &nameThis); err != nil {
		log.Fatal(err)
	}

	channel := nameThis.Channel

	// TODO - make this a working query
	//rows, pgErr := h.postgresClient.Query(r.Context(), "insert into channels (uuid, name) values ($1, $2)", newUUID(), channel)
	rows, pgErr := h.postgresClient.Query(r.Context(), "insert into channels (uuid, name) VALUES ($1, $2)", newUUID(), channel)

	// TODO - create handlePgErr func to deal with pgx error types (https://pkg.go.dev/github.com/jackc/pgx/v5#pkg-variables)
	_, _ = rows, pgErr
}

func unmarshalInto(readCloser io.ReadCloser, v any) error {
	body, err := io.ReadAll(readCloser)
	if err != nil {
		log.Print(err)
		return err
	}
	if err = json.Unmarshal(body, v); err != nil {
		log.Println(err)
		return err
	}
	return err
}

// JS should have a record of the last sent message for client OR that can be put in the database for the user.
// getNewMessagesHandler should retrieve messages since that timestamp and paginate the results
// For now, a 10 second poll is sent for new messages

// validate channel in postChatMessage by checking the DB

func validateChannel() {}

// Renames a channel since only the uuid matters
func renameChannel() {}

// The user who creates the channel is marked in the channels table under the admin field. That field allows that user to rename the channel for everyone

func produceMessage(p sarama.AsyncProducer, message *sarama.ProducerMessage) error {

	p.Input() <- message
	return nil
}
