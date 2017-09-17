package rig

import "encoding/json"

type Rig struct {
	d         *doer
	commitLog *rigLog

	// auth token
	token string
}

type Operation interface {
	Method() string
	Data() json.RawMessage
}

// New returns a new Rig.
func New(logDir string, service Service, applyCommits bool, token, peer string) (*Rig, error) {
	commitLog, err := newRigLog(logDir, token, service, applyCommits)
	if err != nil {
		return nil, err
	}
	d, err := newDoer(commitLog, peer, token)
	if err != nil {
		return nil, err
	}
	return &Rig{
		d:         d,
		commitLog: commitLog,
	}, nil
}
