package inttest

import (
	"context"
	"sync"
	"time"

	"github.com/rchapin/go-in-mem-datastore/inmemdatastore"
	log "github.com/rchapin/rlog"
)

// The mode in which we will run the test
type Mode int64

const (
	// Indicates that we are passing in a limited key space to use for the id field in each record
	// that we generate.
	LimitedKeySpace = iota
	// Indicated that we are passing in specific records in specific order that we want to have the
	// Writers use for the test.
	SpecificRecords
)

type TRConfig struct {
	// The mode in which the test is to run
	mode Mode
	// Specific records that each writer, by its id number, should attempt to write to the IMDS.
	specificRecords map[int][]map[string]interface{}
	// The number of Datastore shards
	numDatastoreShards int
	// The size of the IMDS serialization channel buffer size that we should pass into the IMDS.
	persistenceChanBuffSize int
	// Then number of Serializers to be passed to the IMDS.
	numPersisters int
	// The number of Writers that we will instantiate and kick-off to write to the IMDS.
	numTestWriters int
	// The number of records that each Writer will write before shutting down.
	numTestWriterWrites int
	// The amount of time in milliseconds that the Writers will sleep between writes.
	testWriterSleepTime int64
	// The number of Readers that we will instantiate and kick-off to read from the IMDS.
	numTestReaders int
	// The amount of time in milliseconds that the Readers will sleep between reads.
	testReaderSleepTime int64
	// The avro schema, in "raw" string form that we will pass to the IMDS.
	schema string
	// The directory into which we will tell the IMDS to write its data files
	outputDirPath string
	// The keys that we expect to be written to the datastore.  We will provide these to all of the
	// readers so that they can randomly query the datastore for records.
	keySpace []string
	// The number of double and string fields for which we need to generate test data based on the
	// structure of the test avro schema.
	numDblFields int
	numStrFields int
}

type TestRunner struct {
	ctx       context.Context
	cancel    context.CancelFunc
	wg        *sync.WaitGroup
	cfg       TRConfig
	imdsWg    *sync.WaitGroup
	imds      *inmemdatastore.InMemDataStore
	writers   map[int]*Writer
	readers   map[int]*Reader
	readerWg  *sync.WaitGroup
	StartTime int64
	EndTime   int64
}

func NewTestRunner(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, cfg TRConfig) *TestRunner {
	imdsWg := &sync.WaitGroup{}
	retval := &TestRunner{
		ctx:      ctx,
		cancel:   cancel,
		wg:       wg,
		cfg:      cfg,
		writers:  make(map[int]*Writer),
		readers:  make(map[int]*Reader),
		readerWg: &sync.WaitGroup{},
		imdsWg:   imdsWg,
	}
	retval.imds = initTestOut(ctx, cancel, cfg, imdsWg)
	return retval
}

func (tr *TestRunner) RunTest() {
	tr.StartTime = time.Now().UTC().Unix()
	tr.imds.Start()
	tr.startReaders()
	switch tr.cfg.mode {
	case SpecificRecords:
		tr.execSpecificRecordsTest()
	case LimitedKeySpace:
		tr.execLimitedKeySpaceTest()
	}

	tr.wg.Wait()
	log.Info("TestRunner complete")
	tr.EndTime = time.Now().UTC().Unix()
	tr.imds.Shutdown()
	tr.imdsWg.Wait()
}

func (tr *TestRunner) execSpecificRecordsTest() {
	log.Info("Executing specific records test")

	for wrkrId, records := range tr.cfg.specificRecords {
		// Create WriterConfigs for them to write a specific set of records
		writerCfg := WriterConfig{
			workerCfg: WorkerConfig{
				Id:   wrkrId,
				IMDS: tr.imds,
				Wg:   tr.wg,
			},
			mode:            tr.cfg.mode,
			SpecificRecords: records,
			sleepTime:       tr.cfg.testWriterSleepTime,
			numDblFields:    tr.cfg.numDblFields,
			numStrFields:    tr.cfg.numStrFields,
		}
		writer := NewWriter(writerCfg)
		tr.writers[wrkrId] = writer
		writer.Start()
	}
	log.Infof("All writers have been started")

	// Wait on what amounts to the writers to finish writing and then call cancel.
	// That will cause the IMDS to cancel and begin the shutdown process.
	tr.wg.Wait()
	tr.cancel()
	tr.readerWg.Wait()
}

func (tr *TestRunner) execLimitedKeySpaceTest() {
	log.Info("Executing limited key space test")

	for i := 0; i < tr.cfg.numTestWriters; i++ {
		// Create WriterConfigs for them to write to a random set of keys with randomly generated
		// data.
		writerCfg := WriterConfig{
			workerCfg: WorkerConfig{
				Id:       i,
				IMDS:     tr.imds,
				Wg:       tr.wg,
				KeySpace: tr.cfg.keySpace,
			},
			mode:         tr.cfg.mode,
			sleepTime:    tr.cfg.testWriterSleepTime,
			numWrites:    tr.cfg.numTestWriterWrites,
			numDblFields: tr.cfg.numDblFields,
			numStrFields: tr.cfg.numStrFields,
		}
		writer := NewWriter(writerCfg)
		tr.writers[i] = writer
		writer.Start()
	}
	log.Infof("All writers have been started")

	// Wait on what amounts to the writers to finish writing and then call cancel.
	// That will cause the IMDS to cancel and begin the shutdown process.
	tr.wg.Wait()
	tr.cancel()
	tr.readerWg.Wait()
}

func initTestOut(ctx context.Context, cancel context.CancelFunc, cfg TRConfig, imdsWg *sync.WaitGroup) *inmemdatastore.InMemDataStore {
	persisters := make(map[int]*inmemdatastore.Persister)
	persistanceChanBuffSize := 1024
	persistenceChan := make(inmemdatastore.PersistenceChan, persistanceChanBuffSize)

	for i := 0; i < cfg.numPersisters; i++ {
		avroWriterCfg := inmemdatastore.AvroFileWriterConfig{
			WriterCfg:  inmemdatastore.WriterCfg{Id: i, OutputDir: cfg.outputDirPath},
			AvroSchema: cfg.schema,
		}
		avroFileWriter := inmemdatastore.NewAvroFileWriter(ctx, imdsWg, avroWriterCfg)
		serializer := inmemdatastore.NewNoopSerializer()
		persisterConfig := inmemdatastore.PersisterConfig{
			Id:         i,
			Serializer: serializer,
			Writer:     avroFileWriter,
			InputChan:  persistenceChan,
		}
		persister := inmemdatastore.NewPersister(ctx, imdsWg, persisterConfig)
		persisters[i] = persister
	}

	// Create the cache persister and channel
	cachePersisterChan := make(inmemdatastore.PersistenceChan, persistanceChanBuffSize)
	cachePersisterAvroWriterCfg := inmemdatastore.AvroFileWriterConfig{
		WriterCfg:  inmemdatastore.WriterCfg{Id: 0, OutputDir: cfg.outputDirPath, FileNameSuffix: "cache"},
		AvroSchema: cfg.schema,
	}
	cachePersisterSerializer := inmemdatastore.NewNoopSerializer()
	cachePersisterAvroFileWriter := inmemdatastore.NewAvroFileWriter(ctx, imdsWg, cachePersisterAvroWriterCfg)
	persisterConfig := inmemdatastore.PersisterConfig{
		Id:         1,
		Serializer: cachePersisterSerializer,
		Writer:     cachePersisterAvroFileWriter,
		InputChan:  cachePersisterChan,
	}
	cachePersister := inmemdatastore.NewPersister(ctx, imdsWg, persisterConfig)

	imdsCfg := inmemdatastore.Config{
		NumDatastoreShards: cfg.numDatastoreShards,
		RecordTimestampKey: recordTimestampKey,
		PersistenceChan:    persistenceChan,
		Persisters:         persisters,
		// Additional Persister instance and a shutdown channel
		// Persister needs to have an optional file name added to it
		// into which that Persister will write the saved cache data
		CachePersister:     cachePersister,
		CachePersisterChan: cachePersisterChan,
	}
	return inmemdatastore.NewInMemDatastore(ctx, cancel, imdsWg, imdsCfg)
}

func (tr *TestRunner) startReaders() {
	for i := 0; i < tr.cfg.numTestReaders; i++ {
		readerCfg := ReaderConfig{
			workerCfg: WorkerConfig{
				Id:       i,
				IMDS:     tr.imds,
				Wg:       tr.readerWg,
				KeySpace: tr.cfg.keySpace,
			},
			sleepTime: tr.cfg.testReaderSleepTime,
			ctx:       tr.ctx,
		}
		reader := NewReader(readerCfg)
		tr.readers[i] = reader
		reader.Run()
	}
}
