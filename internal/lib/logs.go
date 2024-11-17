package lib

import (
	"github.com/rs/zerolog"
	"os"
)

// See https://github.com/rs/zerolog

// Should I use a type alias or a struct here?

type Logger struct {
	logger *zerolog.Logger
}

func (logger *Logger) Debug(str string) {
	logger.logger.Debug().Msg(str)
}

func (logger *Logger) Info(str string) {
	logger.logger.Info().Msg(str)
}

func (logger *Logger) Warn(str string) {
	logger.logger.Warn().Msg(str)
}

func (logger *Logger) Error(str string, err error) {
	logger.logger.Error().Err(err).Msg(str)
}

func (logger *Logger) Fatal(str string, err error) {
	// TODO - stack trace on fatal/panic
	logger.logger.Fatal().Err(err).Msg(str)
}

func (logger *Logger) Panic(str string, err error) {
	logger.logger.Panic().Err(err).Msg(str)
}

func NewLogger(serviceName string, serviceVersion string, logLevel string) *Logger {
	var (
		zerologLevel zerolog.Level
		logger       = zerolog.New(os.Stdout).With().
				Str("serviceName", serviceName).
				Str("serviceVersion", serviceVersion).Logger()
	)

	// Map string log levels to slog log levels whilst accepting lowercase and uppercase forms
	// See https://go.dev/wiki/Switch
	switch logLevel {
	case "debug", "DEBUG":
		zerologLevel = zerolog.DebugLevel
	case "info", "INFO":
		zerologLevel = zerolog.InfoLevel
	case "warn", "WARN":
		zerologLevel = zerolog.WarnLevel
	case "error", "ERROR":
		zerologLevel = zerolog.ErrorLevel
	}

	zerolog.SetGlobalLevel(zerologLevel)

	return &Logger{logger: &logger}
	// TODO - tracing integration https://github.com/rs/zerolog#contextcontext-integration
}
