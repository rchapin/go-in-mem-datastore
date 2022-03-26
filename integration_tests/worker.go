//go:build integration

package inttest

import (
	"sync"

	"github.com/rchapin/go-in-mem-datastore/inmemdatastore"
)

type WorkerConfig struct {
	Id       int
	IMDS     *inmemdatastore.InMemDataStore
	Wg       *sync.WaitGroup
	KeySpace []string
}
type Worker struct {
	id       int
	imds     *inmemdatastore.InMemDataStore
	wg       *sync.WaitGroup
	times    []int64
	keySpace []string
}

func NewWorker(cfg WorkerConfig) *Worker {
	return &Worker{
		id:       cfg.Id,
		imds:     cfg.IMDS,
		wg:       cfg.Wg,
		keySpace: cfg.KeySpace,
	}
}

// GetRandomKey will pull a random string from the keySpace slice
func (w *Worker) getRandomKey() string {
	idx := getRandomInt(0, len(w.keySpace))
	return w.keySpace[idx]
}
