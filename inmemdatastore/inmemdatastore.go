package inmemdatastore

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	log "github.com/rchapin/rlog"
)

const (
	RecFieldId = "id"
)

type (
	PersistenceChan chan map[string]interface{}
	Persisters      map[int]*Persister
	Datastores      map[uint64]*Datastore
	Config          struct {
		NumDatastoreShards      int
		PersistenceChan         PersistenceChan
		PersistanceChanBuffSize int
		Persisters              Persisters
		// The top-level key in the map[string]interface{} records that will be stored in the
		// InMemoryDataStore.  This is required for the Put method to process a new record.
		RecordTimestampKey string
		CachePersister     *Persister
		CachePersisterChan PersistenceChan
	}
)

type Datastore struct {
	Id        uint64
	Data      map[string]interface{}
	NumReads  int64
	NumWrites int64
	mux       *sync.RWMutex
}

func NewDatastore(id uint64) *Datastore {
	return &Datastore{
		Id:        id,
		Data:      make(map[string]interface{}),
		NumReads:  0,
		NumWrites: 0,
		mux:       &sync.RWMutex{},
	}
}

type InMemDataStore struct {
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 *sync.WaitGroup
	datastores         Datastores
	numShards          int
	recordTimestampKey string
	persisters         Persisters
	startTime          int64
	persisterCtx       context.Context
	persisterCancel    context.CancelFunc
	persistenceChan    PersistenceChan
	// A singleton Persister that will be used to serialize all of the data in the cache on
	// Shutdown.
	CachePersister Persister
	// A channel from which the cachePersister will read all of the records in the cache.
	CachePersisterChan PersistenceChan
}

func NewInMemDatastore(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, cfg Config) *InMemDataStore {
	// Create a separate context and cancelFunc pair that we will use to manage the Persisters.
	// Otherwise, if the "parent" cancelFunc is invoked it will cancel both the IMDS and the
	// Persisters and the IMDS will not be able to block and wait for the Persisters to cleanly
	// close before exiting.
	persisterCtx, persisterCancel := context.WithCancel(context.Background())

	retval := &InMemDataStore{
		ctx:                ctx,
		cancel:             cancel,
		wg:                 wg,
		datastores:         make(Datastores, cfg.NumDatastoreShards),
		numShards:          cfg.NumDatastoreShards,
		recordTimestampKey: cfg.RecordTimestampKey,
		persisters:         cfg.Persisters,
		persistenceChan:    cfg.PersistenceChan,
		persisterCtx:       persisterCtx,
		persisterCancel:    persisterCancel,
	}
	for i := uint64(0); i < uint64(retval.numShards); i++ {
		retval.datastores[i] = NewDatastore(i)
	}
	return retval
}

func (ds *InMemDataStore) Get(key string) (interface{}, error) {
	datastore, err := ds.getDatastoreShard(key)
	if err != nil {
		return nil, err
	}
	datastore.mux.RLock()
	rec := datastore.Data[key]
	datastore.NumReads++
	datastore.mux.RUnlock()
	return rec, nil
}

// GetAll will return the entire in memory data store.  For a production grade piece of software we
// would have to do something other than just returning a reference to the private map, such as a
// deep copy.  For now, this is just here to facilitate testing.
func (ds *InMemDataStore) GetAll() map[string]interface{} {
	retval := make(map[string]interface{})
	for _, datastore := range ds.datastores {
		for k, v := range datastore.Data {
			retval[k] = v
		}
	}
	return retval
}

func (ds *InMemDataStore) GetDatastores() Datastores {
	return ds.datastores
}

func (ds *InMemDataStore) persistCache() {
	ds.wg.Add(1)
	defer ds.wg.Done()
}

func (ds *InMemDataStore) Put(key string, val map[string]interface{}) error {
	datastore, err := ds.getDatastoreShard(key)
	if err != nil {
		return err
	}
	datastore.mux.Lock()

	// Here we validate that we do not yet have a record in the datastore that is newer than this
	// one.  It is entirely possible, given multiple concurrent writes that a record was put into
	// the datastore that is newer than the current one that we are trying to write.  In that case,
	// we need to verify that there isn't a record that is newer.  Otherwise, we will not be
	// reflecting the current state of the system.  Regardless, we still want to persist this record
	// to disk.k
	//
	// First, attempt to get this record from the datastore
	data := datastore.Data
	rec, ok := data[key]
	if !ok {
		// We don't yet have a record for this key in the datastore at all, just write it and continue
		data[key] = val
	} else {
		// Get the collection_time from the existing record and ensure that it is older
		recMap := rec.(map[string]interface{})
		existingTimestamp, ok := recMap[ds.recordTimestampKey].(int64)
		if !ok {
			// There is no top-level key in the recMap pulled from the cache to which we can compare
			// timestamps, we will just write it.
			// TODO: add some sort of stat that we can return to the caller
			data[key] = val
		} else {
			if existingTimestamp < val[ds.recordTimestampKey].(int64) {
				// The incoming record is newer than what we currently have in the cache, we should
				// persist this in the cache.
				data[key] = val
			}
		}
	}

	datastore.NumWrites++
	numWrites := datastore.NumWrites
	datastore.mux.Unlock()
	if numWrites%500 == 0 {
		log.Infof("IMDS Datastore writes, id=%d, numWrites=%d", datastore.Id, numWrites)
	}

	ds.persistenceChan <- val
	return nil
}

// Start will spin up the Persisters and when it returns will be ready for reads and writes.
func (ds *InMemDataStore) Start() {
	ds.startTime = time.Now().UTC().UnixMilli()
	for _, persister := range ds.persisters {
		persister.Run()
	}
}

// Shutdown will signal the Serializers to close their open file handles and shutdown the IMDS.
func (ds *InMemDataStore) Shutdown() {
	log.Info("Shutdown command received, shutting down serializers")
	ds.persisterCancel()
	ds.persistCache()
	log.Info("Waiting for all persisters to finish shutting down")
	ds.wg.Wait()
	log.Info("Shutdown complete")
}

func (ds *InMemDataStore) getDatastoreShard(key string) (*Datastore, error) {
	shardId := GetDatastoreShardId(key, ds.numShards)
	datastore, ok := ds.datastores[shardId]
	if !ok {
		return nil, fmt.Errorf("unable to resolve datastore; key=%s, shardId=%d", key, shardId)
	}
	return datastore, nil
}

func GetDatastoreShardId(key string, numShards int) uint64 {
	hasher := fnv.New64a()
	hasher.Write([]byte(key))
	hash := hasher.Sum64()
	return hash % uint64(numShards)
}
