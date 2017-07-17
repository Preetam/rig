package rig

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/Preetam/lm2log"
	"github.com/Preetam/rig/client"
	"github.com/Preetam/rig/middleware"
	"github.com/Preetam/siesta"
)

type Doer struct {
	lock      sync.Mutex
	commitLog *Log
	peer      *client.LogClient

	errCount int
}

func NewDoer(commitLog *Log, peer string) (*Doer, error) {
	doer := &Doer{
		commitLog: commitLog,
	}

	if peer != "" {
		doer.peer = client.NewLogClient(peer)

		peerCommitted, err := doer.peer.Committed()
		if err != nil {
			if err != lm2log.ErrNotFound {
				return nil, err
			}
		}
		peerCommittedVersion := peerCommitted.Version

		localCommitted, err := commitLog.Committed()
		if err != nil {
			if err.(LogError).StatusCode != http.StatusNotFound {
				return nil, err
			}
		}
		localCommittedVersion := localCommitted.Version

		// Check if peer is behind or caught up (special case).
		if peerCommittedVersion <= localCommittedVersion {
			// It is not. If it is, the loop below does nothing.
			for i := peerCommittedVersion; i != localCommittedVersion; i++ {
				// Get the ith record.
				payload, err := commitLog.Record(i + 1)
				if err != nil {
					log.Println(err)
					return nil, err
				}
				err = doer.peer.Prepare(payload)
				if err != nil {
					log.Println(err)
					return nil, err
				}
				err = doer.peer.Commit()
				if err != nil {
					log.Println(err)
					return nil, err
				}
			}
		} else {
			// Peer is ahead.
			err = commitLog.Rollback()
			if err != nil {
				return nil, err
			}
			for i := localCommittedVersion; i != peerCommittedVersion; i++ {
				// Get the ith record.
				payload, err := doer.peer.GetRecord(i + 1)
				if err != nil {
					return nil, err
				}
				err = commitLog.Prepare(payload)
				if err != nil {
					return nil, err
				}
				err = commitLog.Commit()
				if err != nil {
					return nil, err
				}
			}
		}

		// Now the committed versions are synced up. It's time to handle the prepared case.

		peerPrepared, err := doer.peer.Prepared()
		if err != nil {
			if err != lm2log.ErrNotFound {
				return nil, err
			}
		}
		peerPreparedVersion := peerPrepared.Version

		localPrepared, err := commitLog.Prepared()
		if err != nil {
			if err.(LogError).StatusCode != http.StatusNotFound {
				return nil, err
			}
		}
		localPreparedVersion := localPrepared.Version

		if localPreparedVersion > 0 || peerPreparedVersion > 0 {
			// Something was prepared and not completed.
			// Roll them back.
			err = commitLog.Rollback()
			if err != nil {
				return nil, err
			}
			err = doer.peer.Rollback()
			if err != nil {
				return nil, err
			}
		}
	}
	err := commitLog.Commit()
	if err != nil {
		return nil, err
	}
	return doer, nil
}

func (d *Doer) Do(p client.LogPayload, ignoreVersion bool) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	handleErr := func(err error) error {
		if err != nil {
			d.errCount++
			if d.errCount == 3 {
				log.Fatal("Reached max error count; shutting down.")
			}
		}
		return err
	}

	err := d.commitLog.LockResources(p.Op)
	if err != nil {
		return err
	}
	defer d.commitLog.UnlockResources(p.Op)

	committedPayload, err := d.commitLog.Committed()
	if err != nil {
		if err.(LogError).Err != nil {
			log.Println("couldn't get prepared version:", err)
			return handleErr(err)
		}
	}

	if ignoreVersion {
		p.Version = committedPayload.Version + 1
	}

	err = d.commitLog.Prepare(p)
	if err != nil {
		log.Println("couldn't prepare locally:", err)
		return handleErr(err)
	}

	if d.peer != nil {
		err = d.peer.Prepare(p)
		if err != nil {
			log.Println("couldn't prepare on peer:", err)
			d.commitLog.Rollback()
			return handleErr(err)
		}
	}

	err = d.commitLog.Commit()
	if err != nil {
		log.Println("couldn't commit locally:", err)
		return handleErr(err)
	}

	if d.peer != nil {
		err = d.peer.Commit()
		if err != nil {
			log.Println("couldn't commit on peer:", err)
			return handleErr(err)
		}
	}

	return handleErr(nil)
}

func (d *Doer) Handler() func(c siesta.Context, w http.ResponseWriter, r *http.Request) {
	return func(c siesta.Context, w http.ResponseWriter, r *http.Request) {
		requestData := c.Get(middleware.RequestDataKey).(*middleware.RequestData)

		var params siesta.Params
		ignoreVersion := params.Bool("ignore-version", true, "Ignore version in payload")
		err := params.Parse(r.Form)
		if err != nil {
			requestData.ResponseError = err.Error()
			requestData.StatusCode = http.StatusBadRequest
			return
		}

		var doPayload client.LogPayload
		err = json.NewDecoder(r.Body).Decode(&doPayload)
		if err != nil {
			requestData.ResponseError = err.Error()
			requestData.StatusCode = http.StatusBadRequest
			return
		}

		err = d.Do(doPayload, *ignoreVersion)
		if err != nil {
			requestData.ResponseError = err.Error()
			if logErr, ok := err.(LogError); ok {
				requestData.StatusCode = logErr.StatusCode
			} else {
				requestData.StatusCode = http.StatusInternalServerError
			}
			log.Printf("[Req %s] error %v", requestData.RequestID, err)
			return
		}
	}
}
