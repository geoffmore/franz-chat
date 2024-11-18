package lib

import (
	"errors"
	"github.com/jackc/pgx/v5/pgconn"
	"log"
)

// Use LISTEN/NOTIFY here to init connection

// HandlePGError expects a specific pgx error and returns nil if matched and the error otherwise
func HandlePGError(given, expected error) error {
	// See https://github.com/jackc/pgx/wiki/Error-Handling
	var pgErr *pgconn.PgError
	if given == nil || errors.Is(given, expected) {
		return nil
	}
	if errors.As(given, &pgErr) {
		// TODO - improve these errors
		log.Println(pgErr.Message)
		log.Println(pgErr.Code)
	}
	return given
}
