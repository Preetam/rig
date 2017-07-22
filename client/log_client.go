package client

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Preetam/lm2log"
	"github.com/Preetam/rig/middleware"
)

type LogClient struct {
	client *Client
}

// LogPayload is request payload for log operations.
type LogPayload struct {
	Version uint64    `json:"version"`
	Op      Operation `json:"op"`
}

// Operation represents a log operation.
type Operation struct {
	Method string          `json:"method"`
	Data   json.RawMessage `json:"data"`
}

// NewOperation returns a new Operation.
func NewOperation() Operation {
	return Operation{}
}

func NewLogClient(baseURI string) *LogClient {
	return &LogClient{
		client: New(baseURI, middleware.Token),
	}
}

func (c *LogClient) Prepared() (LogPayload, error) {
	payload := LogPayload{}
	resp := middleware.APIResponse{
		Data: &payload,
	}
	err := c.client.doRequest("GET", "/prepare", nil, &resp)
	if err != nil {
		if serverErr, ok := err.(ServerError); ok {
			if serverErr == http.StatusNotFound {
				return payload, lm2log.ErrNotFound
			}
		}
		return payload, resp
	}
	return payload, nil
}

func (c *LogClient) Committed() (LogPayload, error) {
	payload := LogPayload{}
	resp := middleware.APIResponse{
		Data: &payload,
	}
	err := c.client.doRequest("GET", "/commit", nil, &resp)
	if err != nil {
		if serverErr, ok := err.(ServerError); ok {
			if serverErr == http.StatusNotFound {
				return payload, lm2log.ErrNotFound
			}
		}
		return payload, resp
	}
	return payload, nil
}

func (c *LogClient) Prepare(payload LogPayload) error {
	err := c.client.doRequest("POST", "/prepare", &payload, nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *LogClient) Commit() error {
	return c.client.doRequest("POST", "/commit", nil, nil)
}

func (c *LogClient) Rollback() error {
	return c.client.doRequest("POST", "/rollback", nil, nil)
}

func (c *LogClient) GetRecord(version uint64) (LogPayload, error) {
	p := LogPayload{}
	resp := middleware.APIResponse{
		Data: &p,
	}
	err := c.client.doRequest("GET", fmt.Sprintf("/record/%d", version), nil, &resp)
	if err != nil {
		return p, err
	}
	return p, nil
}
