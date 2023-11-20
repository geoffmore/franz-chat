package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/peterbourgon/ff/v3"
	"go.opentelemetry.io/otel/semconv/v1.13.0/httpconv"
	oteltrace "go.opentelemetry.io/otel/trace"
	"io"
	"log"
	"net/http"
)

const (
	chatTopic = "chat"
	appName   = "franz-api"
)

func main() {
	var (
		a                  = newApp()
		kafkaConnection    = flag.String("kafka.connection", "localhost:9094", "Kafka connection string")
		postgresConnection = flag.String("postgres.connection", "postgresql://franz_api:franz_api@localhost:5432/franz?application_name=franz_api", "Postgres connection")
		port               = flag.Int("port", 8008, "Listen port")
	)

	if err := ff.Parse(flag.CommandLine, nil); err != nil {
		log.Fatal(err)
	}

	// Init tracing
	if true { // TODO - wrap in trace enablement feature flag
		if err := a.RegisterTracer(appName); err != nil {
			log.Fatal(err)
		}
	}

	// Init configs
	kafkaCfg := newKafkaConfig(kafkaConnection)
	if a.tracer != nil {
		kafkaCfg.tracer = a.tracer
	}

	// Init stateful connections
	a.AsyncProducer = newAsyncProducer(kafkaCfg)
	// a.SyncProducer = newSyncProducer(kafkaCfg)
	// a.kafkaConsumerGroup = newKafkaConsumerGroup(kafkaConnection, appName)
	a.pgPool = newPostgresConnPool(postgresConnection)

	s := newService()

	// Set up routes
	a.setupRoutes(s)
	s.Start(port)

	defer func() {
		if err := a.Close(); err != nil {
			log.Println("failed to close server connections")
		}
	}()
}

type PostChatRequest struct {
	// How does backend capture expected json content with struct tags?
	// Maybe I unmarshal into an interface and ask the question "Does the json object have the fields I need? and validate those fields against some struct or something
	Channel string `json:"channel,omitempty"`
	Msg     string `json:"msg,omitempty"`
}

func (a *app) postChatHandler(r *http.Request) (int, interface{}) {
	var (
		ctx             = r.Context()
		postChatRequest PostChatRequest
	)

	// Does the channel exist?
	// Is the message bad?

	// TODO - maybe rename this to "schema" and make it a struct literal?
	// TODO - rename spanName to "method http.route" to conform with spec
	/* TODO - figure out how to conform to https://opentelemetry.io/docs/specs/semconv/http/http-spans/ while also
	moving away from the deprecated httpconv (https://github.com/open-telemetry/opentelemetry-go/releases/tag/v1.17.0)
	*/
	// TODO - figure out how to separate a route from a path (route should not be rendered as path)
	ctx, span := a.tracer.Start(ctx, fmt.Sprintf("%s %s", r.Method, r.URL.Redacted()),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
		oteltrace.WithAttributes(httpconv.ServerRequest("", r)...),
	)
	defer span.End()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println("unable to read body")
	}
	if err := json.Unmarshal(body, &postChatRequest); err != nil {
		log.Println("unable to unmarshal json body")
	}

	a.AsyncProducer.produceMessage(ctx,
		&sarama.ProducerMessage{
			Topic: chatTopic,
			Value: sarama.StringEncoder(postChatRequest.Msg),
		},
	)

	if err != nil {
		log.Print(err)
	}
	return httpJSONOk() // should be the expected response
}

type PostCreateChannelRequest struct {
	Channel string `json:"channel"`
}

func (a *app) postCreateChannelHandler(r *http.Request) (int, interface{}) {
	var (
		ctx    = r.Context()
		schema PostCreateChannelRequest
	)

	ctx, span := a.tracer.Start(ctx, fmt.Sprintf("%s %s", r.Method, r.URL.Redacted()),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
		oteltrace.WithAttributes(httpconv.ServerRequest("", r)...),
	)
	defer span.End()

	if err := unmarshalInto(r.Body, &schema); err != nil {
		log.Fatal(err)
	}

	rows, pgErr := a.pgPool.Query(r.Context(), "insert into channels (uuid, name) VALUES ($1, $2)", newUUID(), schema.Channel)

	// TODO - create handlePgErr func to deal with pgx error types (https://pkg.go.dev/github.com/jackc/pgx/v5#pkg-variables)
	_, _ = rows, pgErr

	return httpJSONOk() // should be the expected response
}

type PostCreateUserRequest struct {
	User string `json:"user"`
}

func (a *app) postCreateUserHandler(r *http.Request) (int, interface{}) {
	var (
		ctx    = r.Context()
		schema PostCreateUserRequest
	)

	ctx, span := a.tracer.Start(ctx, fmt.Sprintf("%s %s", r.Method, r.URL.Redacted()),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
		oteltrace.WithAttributes(httpconv.ServerRequest("", r)...),
	)
	defer span.End()

	if err := unmarshalInto(r.Body, &schema); err != nil {
		log.Fatal(err)
	}

	rows, pgErr := a.pgPool.Query(r.Context(), "insert into users (uuid, name) VALUES ($1, $2)", newUUID(), schema.User)

	// TODO - create handlePgErr func to deal with pgx error types (https://pkg.go.dev/github.com/jackc/pgx/v5#pkg-variables)
	_, _ = rows, pgErr

	return httpJSONOk() // should be the expected response
}

func (a *app) setupRoutes(s service) {
	s.POST("/chat",
		a.postChatHandler,
		midNoOp(),
		midContentType("application/json"),
	)
	s.POST("/create/channel",
		a.postCreateChannelHandler,
	)
	s.POST("/create/user",
		a.postCreateUserHandler,
	)
}
