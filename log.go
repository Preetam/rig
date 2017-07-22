package rig

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/Preetam/lm2log"
	"github.com/Preetam/rig/client"
	"github.com/Preetam/rig/middleware"
	"github.com/Preetam/siesta"
)

type LogError struct {
	// Error type
	Type string
	// HTTP status code
	StatusCode int
	// Underlying error, if any.
	Err error
}

func (err LogError) Error() string {
	return fmt.Sprintf("LogError [%s-%d] (%s)", err.Type, err.StatusCode, err.Err)
}

type Service interface {
	Validate(client.Operation) error
	Apply(uint64, client.Operation) error
	LockResources(client.Operation) bool
	UnlockResources(client.Operation)
}

// Log represents a commit log.
type Log struct {
	// service is the service being modified.
	service Service
	// apply determines whether or not commits are
	// applied to the service.
	applyCommits bool
	// commitLog represents the actual log on disk.
	commitLog *lm2log.Log

	lock sync.Mutex
}

func NewLog(logDir string, service Service, applyCommits bool) (*Log, error) {
	collectionPath := filepath.Join(logDir, "log")
	err := os.MkdirAll(collectionPath, 0755)
	if err != nil {
		return nil, err
	}
	commitLog, err := lm2log.Open(filepath.Join(collectionPath, "log.lm2"))
	if err != nil {
		if err == lm2log.ErrDoesNotExist {
			commitLog, err = lm2log.New(filepath.Join(collectionPath, "log.lm2"))
		}
		if err != nil {
			return nil, LogError{Type: "commitlog_new", Err: err}
		}
	}

	return &Log{
		service:      service,
		applyCommits: applyCommits,
		commitLog:    commitLog,
	}, nil
}

func (l *Log) Prepared() (client.LogPayload, error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	var p client.LogPayload

	preparedVersion, err := l.commitLog.Prepared()
	if err != nil {
		if err == lm2log.ErrNotFound {
			return p, LogError{
				Type:       "internal",
				Err:        err,
				StatusCode: http.StatusNotFound,
			}
		}
		return p, LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	preparedData, err := l.commitLog.Get(preparedVersion)
	if err != nil {
		return p, LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	operation := client.NewOperation()
	err = json.Unmarshal([]byte(preparedData), &operation)
	if err != nil {
		return p, LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	p = client.LogPayload{
		Version: preparedVersion,
		Op:      operation,
	}

	return p, nil
}

func (l *Log) Committed() (client.LogPayload, error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	var p client.LogPayload

	committedVersion, err := l.commitLog.Committed()
	if err != nil {
		return p, LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	if committedVersion == 0 {
		return p, LogError{
			Type:       "internal",
			Err:        nil,
			StatusCode: http.StatusNotFound,
		}
	}

	committedData, err := l.commitLog.Get(committedVersion)
	if err != nil {
		return p, LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	operation := client.NewOperation()
	err = json.Unmarshal([]byte(committedData), &operation)
	if err != nil {
		return p, LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	p = client.LogPayload{
		Version: committedVersion,
		Op:      operation,
	}

	return p, nil
}

func (l *Log) Prepare(payload client.LogPayload) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	committed, err := l.commitLog.Committed()
	if err != nil {
		return LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	if payload.Version != committed+1 {
		return LogError{
			Type:       "internal",
			Err:        errors.New("preparing invalid version"),
			StatusCode: http.StatusBadRequest,
		}
	}

	err = l.service.Validate(payload.Op)
	if err != nil {
		return LogError{
			Type:       "internal",
			Err:        errors.New("invalid operation"),
			StatusCode: http.StatusBadRequest,
		}
	}

	data, err := json.Marshal(payload.Op)
	if err != nil {
		return LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	err = l.commitLog.Prepare(string(data))
	if err != nil {
		return LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}
	return nil
}

func (l *Log) Commit() error {
	l.lock.Lock()
	defer l.lock.Unlock()

	err := l.commitLog.Commit()
	if err != nil {
		return LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	committedVersion, err := l.commitLog.Committed()
	if err != nil {
		return LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	if committedVersion == 0 {
		return nil
	}

	committedData, err := l.commitLog.Get(committedVersion)
	if err != nil {
		return LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	operation := client.NewOperation()
	err = json.Unmarshal([]byte(committedData), &operation)
	if err != nil {
		return LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	err = l.service.Apply(committedVersion, operation)
	if err != nil {
		return LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	return nil
}

func (l *Log) Rollback() error {
	l.lock.Lock()
	defer l.lock.Unlock()

	err := l.commitLog.Rollback()
	if err != nil {
		return LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	return nil
}

func (l *Log) Record(version uint64) (client.LogPayload, error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	var p client.LogPayload

	committedData, err := l.commitLog.Get(version)
	if err != nil {
		return p, LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	operation := client.NewOperation()
	err = json.Unmarshal([]byte(committedData), &operation)
	if err != nil {
		return p, LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	p = client.LogPayload{
		Version: version,
		Op:      operation,
	}

	return p, nil
}

func (l *Log) LockResources(o client.Operation) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	locked := l.service.LockResources(o)
	if !locked {
		return LogError{
			Type:       "internal",
			Err:        errors.New("resource busy"),
			StatusCode: http.StatusServiceUnavailable,
		}
	}
	return nil
}

func (l *Log) UnlockResources(o client.Operation) {
	l.lock.Lock()
	defer l.lock.Unlock()

	l.service.UnlockResources(o)
}

func (l *Log) Compact(recordsToKeep uint) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	err := l.commitLog.Compact(recordsToKeep)
	if err != nil {
		return LogError{
			Type:       "internal",
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}
	return nil
}

func (l *Log) Service() *siesta.Service {
	commitLog := l.commitLog

	logService := siesta.NewService("/")
	logService.AddPre(middleware.RequestIdentifier)
	logService.AddPre(middleware.CheckAuth)
	logService.AddPost(middleware.ResponseGenerator)
	logService.AddPost(middleware.ResponseWriter)

	logService.Route("GET", "/log/prepare", "", func(c siesta.Context, w http.ResponseWriter, r *http.Request) {
		requestData := c.Get(middleware.RequestDataKey).(*middleware.RequestData)
		payload, err := l.Prepared()
		if err != nil {
			requestData.ResponseError = err.Error()
			requestData.StatusCode = err.(LogError).StatusCode
			return
		}
		requestData.ResponseData = payload
	})

	logService.Route("POST", "/log/prepare", "", func(c siesta.Context, w http.ResponseWriter, r *http.Request) {
		requestData := c.Get(middleware.RequestDataKey).(*middleware.RequestData)
		var preparePayload client.LogPayload

		err := json.NewDecoder(r.Body).Decode(&preparePayload)
		if err != nil {
			requestData.ResponseError = err.Error()
			requestData.StatusCode = http.StatusBadRequest
			return
		}

		err = l.Prepare(preparePayload)
		if err != nil {
			requestData.ResponseError = err.Error()
			requestData.StatusCode = err.(LogError).StatusCode
			return
		}
	})

	logService.Route("GET", "/log/commit", "", func(c siesta.Context, w http.ResponseWriter, r *http.Request) {
		requestData := c.Get(middleware.RequestDataKey).(*middleware.RequestData)
		payload, err := l.Committed()
		if err != nil {
			requestData.ResponseError = err.Error()
			requestData.StatusCode = err.(LogError).StatusCode
			return
		}
		requestData.ResponseData = payload
	})

	logService.Route("POST", "/log/rollback", "", func(c siesta.Context, w http.ResponseWriter, r *http.Request) {
		requestData := c.Get(middleware.RequestDataKey).(*middleware.RequestData)
		err := commitLog.Rollback()
		if err != nil {
			requestData.ResponseError = err.Error()
			requestData.StatusCode = http.StatusInternalServerError
			return
		}
	})

	logService.Route("POST", "/log/commit", "", func(c siesta.Context, w http.ResponseWriter, r *http.Request) {
		requestData := c.Get(middleware.RequestDataKey).(*middleware.RequestData)
		err := l.Commit()
		if err != nil {
			requestData.ResponseError = err.Error()
			requestData.StatusCode = err.(LogError).StatusCode
			return
		}
	})

	logService.Route("GET", "/log/record/:id", "", func(c siesta.Context, w http.ResponseWriter, r *http.Request) {
		requestData := c.Get(middleware.RequestDataKey).(*middleware.RequestData)

		var params siesta.Params
		id := params.Uint64("id", 0, "")
		err := params.Parse(r.Form)
		if err != nil {
			fmt.Fprintln(w, err)
			return
		}
		data, err := commitLog.Get(*id)
		if err != nil {
			requestData.ResponseError = err.Error()
			requestData.StatusCode = http.StatusInternalServerError
			return
		}

		operation := client.NewOperation()
		err = json.Unmarshal([]byte(data), &operation)
		if err != nil {
			requestData.ResponseError = err.Error()
			requestData.StatusCode = http.StatusInternalServerError
			return
		}

		payload := client.LogPayload{
			Version: *id,
			Op:      operation,
		}

		requestData.ResponseData = payload
	})

	logService.Route("POST", "/log/compact", "", func(c siesta.Context, w http.ResponseWriter, r *http.Request) {
		requestData := c.Get(middleware.RequestDataKey).(*middleware.RequestData)
		var params siesta.Params
		keep := params.Uint64("keep", 10000, "Records to keep")
		err := params.Parse(r.Form)
		if err != nil {
			requestData.ResponseError = err.Error()
			requestData.StatusCode = http.StatusBadRequest
			return
		}

		err = l.Compact(uint(*keep))
		if err != nil {
			requestData.ResponseError = err.Error()
			requestData.StatusCode = err.(LogError).StatusCode
			return
		}
	})

	return logService
}
