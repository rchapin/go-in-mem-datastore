package inmemdatastore

import (
	"context"
	"sync"
	"time"

	"github.com/linkedin/goavro"
	log "github.com/rchapin/rlog"
	"github.com/rcrowley/go-metrics"
)

const (
	RecFieldId             = "id"
	RecFieldCollectionTime = "collection_time"
)

type (
	SerializationChan chan map[string]interface{}
	Serializers       map[int]*Serializer
	Config            struct {
		Ctx                      context.Context
		Cancel                   context.CancelFunc
		SerializationChanBufSize int
		NumSerializers           int
		Schema                   string
		OutputDirPath            string
	}
)

type InMemDataStore struct {
	cfg               Config
	ctx               context.Context
	cancel            context.CancelFunc
	serializerCtx     context.Context
	serializerCancel  context.CancelFunc
	wg                *sync.WaitGroup
	serializationChan SerializationChan
	serializers       Serializers
	datastore         map[string]interface{}
	codec             *goavro.Codec
	mux               *sync.RWMutex
	readTimes         []int64
	writeTimes        []int64
	numReads          int64
	numWrites         int64
	startTime         int64
}

func NewInMemDatastore(cfg Config) *InMemDataStore {
	// Create a separate context and cancelFunc pair that we will use to manage the Serializers.
	// Otherwise, if the "parent" cancelFunc is invoked it will both the IMDS and the Serializers to
	// shutdown and the IMDS will not be able to block and wait for the serializers to cleanly close
	// all of the open file handles before exiting.
	ctx, cancel := context.WithCancel(context.Background())

	codec, err := goavro.NewCodec(cfg.Schema)
	if err != nil {
		// TODO: handle this error better.  Or, might be fine if it panics here as passing it an
		// invalid schema won't work regardless.
		panic(err)
	}
	retval := &InMemDataStore{
		cfg:              cfg,
		ctx:              cfg.Ctx,
		cancel:           cfg.Cancel,
		serializerCtx:    ctx,
		serializerCancel: cancel,
		wg:               &sync.WaitGroup{},
		serializers:      make(Serializers, cfg.NumSerializers),
		datastore:        make(map[string]interface{}),
		codec:            codec,
		mux:              &sync.RWMutex{},
	}
	retval.serializationChan = make(chan map[string]interface{}, cfg.SerializationChanBufSize)
	log.Info(cfg.NumSerializers)

	// Keeping track of r/w times.  Will just allocate slices to start.  Ultimately the expansion
	// of the slices could cause additional latency, as this is just a total hack.
	retval.readTimes = []int64{}
	retval.writeTimes = []int64{}
	return retval
}

func (q *InMemDataStore) Get(key string) (interface{}, bool) {
	start := time.Now().UTC().UnixNano()
	q.mux.RLock()
	rec, ok := q.datastore[key]
	q.numReads++
	q.mux.RUnlock()
	duration := time.Now().UTC().UnixNano() - start
	if duration > 0 {
		q.readTimes = append(q.readTimes, duration)
	}
	return rec, ok
}

// GetAll will return the entire in memory data store.  For a production grade piece of software we
// would have to do something other than just returning a reference to the private map, such as a
// deep copy.  For now, this is just here to facilitate testing.
func (q *InMemDataStore) GetAll() map[string]interface{} {
	return q.datastore
}

func (q *InMemDataStore) Put(key string, val map[string]interface{}) error {
	start := time.Now().UTC().UnixNano()
	q.mux.Lock()

	// Here we validate that we do not yet have a record in the datastore that is newer than this
	// one.  It is entirely possible, given multiple concurrent writes that a record was put into
	// the datastore that is newer than the current one that we are trying to write.  In that case,
	// we need to verify that there isn't a record that is newer.  Otherwise, we will not be
	// reflecting the current state of the system.  Regardless, we still want to persist this record
	// to disk.k
	//
	// First, attempt to get this record from the datastore
	rec, ok := q.datastore[key]
	if !ok {
		// We don't yet have a record for this key in the datastore at all, just write it and continue
		q.datastore[key] = val
	} else {
		// Get the collection_time from the existing record and ensure that it is older
		recMap := rec.(map[string]interface{})
		existingTimestamp := recMap[RecFieldCollectionTime].(int64)
		if existingTimestamp < val[RecFieldCollectionTime].(int64) {
			// The incoming record is newer than what we currently have in the cache, we should
			// persist this in the cache.
			q.datastore[key] = val
		}
	}

	q.numWrites++
	q.mux.Unlock()
	duration := time.Now().UTC().UnixNano() - start
	if duration > 0 {
		q.writeTimes = append(q.writeTimes, duration)
	}

	if q.numWrites%1000 == 0 {
		log.Infof("IMDS numWrites=%d", q.numWrites)
	}

	q.serializationChan <- val
	return nil
}

// Start will spin up the specified number of Serializers defined in the configuration and when
// returns will be ready for reads and writes.
func (q *InMemDataStore) Start() {
	q.startTime = time.Now().UTC().UnixMilli()
	for i := 0; i < q.cfg.NumSerializers; i++ {
		serCfg := SerializerConfig{
			ctx:       q.serializerCtx,
			id:        i,
			outputDir: q.cfg.OutputDirPath,
			codec:     q.codec,
			inputChan: q.serializationChan,
			wg:        q.wg,
		}
		serializer := NewSerializer(serCfg)
		q.serializers[i] = serializer
		serializer.Run()
	}
}

// Shutdown will signal the Serializers to close their open file handles and shutdown the IMDS.
func (q *InMemDataStore) Shutdown() {
	log.Info("Shutdown command received, shutting down serializers")
	q.serializerCancel()
	log.Info("Waiting for serializers to finish shutting down")
	q.wg.Wait()
	duration := time.Now().UTC().UnixMilli() - q.startTime
	q.printOverallStats(duration)
	q.printStats(q.readTimes, "read")
	q.printStats(q.writeTimes, "write")
	log.Info("Shutdown complete")
}

func (q *InMemDataStore) printOverallStats(duration int64) {
	writeRate := float64(q.numWrites) / float64(duration)
	readRate := float64(q.numReads) / float64(duration)
	log.Infof("Writes per milli=%f, Reads per milli=%f", writeRate, readRate)
}

func (q *InMemDataStore) printStats(vals []int64, statsType string) {
	s := metrics.NewSampleSnapshot(int64(len(vals)), vals)
	log.Infof("%s stats in milliseconds: min=%v, mean=%v, max=%v",
		statsType,
		float64(s.Min())/1000000,
		s.Mean()/1000000,
		float64(s.Max())/1000000)
	log.Infof("Total reads=%d, writes=%d", q.numReads, q.numWrites)
}
