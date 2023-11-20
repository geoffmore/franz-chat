package main

import (
	"encoding/json"
	"github.com/google/uuid"
	"io"
	"log"
	"net/http"
)

func newUUID() uuid.UUID {
	return uuid.New()
}

type OkResponse struct {
	Msg string `json:"msg"`
}

func httpJSONOk() (int, []byte) {
	b, err := json.Marshal(
		OkResponse{
			Msg: "ok",
		},
	)
	if err != nil {
		log.Fatal("Unable to Marshal OkResponse into bytes!")
	}
	return http.StatusOK, b
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
