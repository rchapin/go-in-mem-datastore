//go:build integration

package inttest

import (
	"context"
	"time"

	log "github.com/rchapin/rlog"
)

type ReaderConfig struct {
	workerCfg WorkerConfig
	sleepTime int64
	ctx       context.Context
}
type Reader struct {
	worker *Worker
	cfg    ReaderConfig
}

func NewReader(cfg ReaderConfig) *Reader {
	retval := &Reader{
		worker: NewWorker(cfg.workerCfg),
		cfg:    cfg,
	}
	return retval
}

func (r *Reader) Run() {
	log.Infof("Reader id=%d starting Run", r.worker.id)
	r.cfg.workerCfg.Wg.Add(1)

	// Setup a ticker to trigger based on the amount of time that we are to wait before attempting a
	// read.  Then kick off a go routine with a select loop that will query the datastore and keep
	// an eye on the context to shutdown.
	ticker := time.NewTicker(time.Duration(r.cfg.sleepTime) * time.Millisecond)
	go func() {
		defer r.cfg.workerCfg.Wg.Done()
		for {
			select {
			case tm := <-ticker.C:
				log.Debugf("Timer triggered, executing query against datastore, tm=%+v", tm)
				key := r.worker.getRandomKey()
				_, ok := r.worker.imds.Get(key)
				if ok {
					log.Debugf("cache hit with key=%s", key)
				} else {
					log.Debugf("cache miss with key=%s", key)
				}
			case <-r.cfg.ctx.Done():
				log.Infof("Reader id=%d exiting Run loop on context done", r.worker.id)
				return
			}
		}
	}()
}
