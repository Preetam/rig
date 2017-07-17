package middleware

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/Preetam/siesta"
)

var Token = ""
var VersionStr = ""

const (
	RequestIDKey     = "request-id"
	StatusCodeKey    = "status-code"
	ResponseDataKey  = "response-data"
	ResponseErrorKey = "response-error"
	ResponseKey      = "response"
	RequestDataKey   = "request-data"
)

type RequestData struct {
	RequestID     string
	StatusCode    int
	ResponseData  interface{}
	ResponseError string
	Response      interface{}
	Start         time.Time
}

type APIResponse struct {
	Data  interface{} `json:"data,omitempty"`
	Error string      `json:"error,omitempty"`
}

func RequestIdentifier(c siesta.Context, w http.ResponseWriter, r *http.Request) {
	requestData := &RequestData{
		RequestID: fmt.Sprintf("%08x", rand.Intn(0xffffffff)),
		Start:     time.Now(),
	}
	log.Printf("[Req %s] %s %s", requestData.RequestID, r.Method, r.URL)
	c.Set(RequestDataKey, requestData)
}

func ResponseGenerator(c siesta.Context, w http.ResponseWriter, r *http.Request) {
	requestData := c.Get(RequestDataKey).(*RequestData)
	response := APIResponse{}

	if data := requestData.ResponseData; data != nil {
		response.Data = data
	}

	response.Error = requestData.ResponseError

	if response.Data != nil || response.Error != "" {
		c.Set(ResponseKey, response)
	}
}

func ResponseWriter(c siesta.Context, w http.ResponseWriter, r *http.Request,
	quit func()) {
	requestData := c.Get(RequestDataKey).(*RequestData)
	if requestData.RequestID != "" {
		w.Header().Set("X-Request-Id", requestData.RequestID)
	}

	w.Header().Set("Content-Type", "application/json")

	enc := json.NewEncoder(w)

	if requestData.StatusCode == 0 {
		requestData.StatusCode = 200
	}
	w.WriteHeader(requestData.StatusCode)

	response := c.Get(ResponseKey)
	if response != nil {
		enc.Encode(response)
	}

	quit()

	log.Printf("[Req %s] status code %d, latency %0.2f ms", requestData.RequestID, requestData.StatusCode,
		time.Now().Sub(requestData.Start).Seconds()*1000)
}

func CheckAuth(c siesta.Context, w http.ResponseWriter, r *http.Request, q func()) {
	requestData := c.Get(RequestDataKey).(*RequestData)
	if Token == "" {
		// No token defined
		return
	}
	if r.Header.Get("X-Api-Key") != Token {
		requestData.StatusCode = http.StatusUnauthorized
		requestData.ResponseError = "invalid token"
		q()
		return
	}
}
