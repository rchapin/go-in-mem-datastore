//go:build integration

package inttest

import (
	"context"
	"sync"

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
	ctx    context.Context
	cancel context.CancelFunc
	// The mode in which the test is to run
	mode Mode

	// Specific records that each writer, by its id number, should attempt to write to the IMDS.
	specificRecords map[int][]map[string]interface{}
	// The size of the IMDS serialization channel buffer size that we should pass into the IMDS.
	serializationChanBufSize int
	// Then number of Serializers to be passed to the IMDS.
	numSerializers int
	// The number of Writers that we will instantiate and kick-off to write to the IMDS.
	numWriters int
	// The number of records that each Writer will write before shutting down.
	numWriterWrites int
	// The amount of time in milliseconds that the Writers will sleep between writes.
	writersSleepTime int64
	// The number of Readers that we will instantiate and kick-off to read from the IMDS.
	numReaders int
	// The amount of time in milliseconds that the Readers will sleep between reads.
	readersSleepTime int64
	// The avro schema, in "raw" string form that we will pass to the IMDS.
	schema string
	wg     *sync.WaitGroup
	// The directory into which we will tell the IMDS to write its data files
	outputDirPath string
	// The keys that we expect to be written to the datastore.  We will provide these to all of the
	// readers so that they can randomly query the datastor for records.
	keySpace []string
}

type TestRunner struct {
	cfg      TRConfig
	imds     *inmemdatastore.InMemDataStore
	writers  map[int]*Writer
	readers  map[int]*Reader
	readerWg *sync.WaitGroup
}

func NewTestRunner(cfg TRConfig) *TestRunner {
	retval := &TestRunner{
		cfg:      cfg,
		writers:  make(map[int]*Writer),
		readers:  make(map[int]*Reader),
		readerWg: &sync.WaitGroup{},
	}
	return retval
}

func (tr *TestRunner) RunTest() {
	IMDSCfg := inmemdatastore.Config{
		Ctx:                      tr.cfg.ctx,
		Cancel:                   tr.cfg.cancel,
		SerializationChanBufSize: tr.cfg.serializationChanBufSize,
		NumSerializers:           tr.cfg.numSerializers,
		Schema:                   tr.cfg.schema,
		OutputDirPath:            tr.cfg.outputDirPath,
	}
	IMDS := inmemdatastore.NewInMemDatastore(IMDSCfg)
	tr.imds = IMDS
	tr.imds.Start()

	tr.startReaders()
	switch tr.cfg.mode {
	case SpecificRecords:
		tr.execSpecificRecordsTest()
	case LimitedKeySpace:
		tr.execLimitedKeySpaceTest()
	}
}

func (tr *TestRunner) execSpecificRecordsTest() {
	log.Info("Executing specific records test")

	for wrkrId, records := range tr.cfg.specificRecords {
		// Create WriterConfigs for them to write a specific set of records
		writerCfg := WriterConfig{
			workerCfg: WorkerConfig{
				Id:   wrkrId,
				IMDS: tr.imds,
				Wg:   tr.cfg.wg,
			},
			mode:            tr.cfg.mode,
			SpecificRecords: records,
			sleepTime:       tr.cfg.writersSleepTime,
		}
		writer := NewWriter(writerCfg)
		tr.writers[wrkrId] = writer
		writer.Start()
	}
	log.Infof("All writers have been started")

	// Wait on what amounts to the writers to finish writing and then call cancel.
	// That will cause the IMDS to cancel and begin the shutdown process.
	tr.cfg.wg.Wait()
	tr.cfg.cancel()
	tr.readerWg.Wait()
}

func (tr *TestRunner) execLimitedKeySpaceTest() {
	log.Info("Executing limited key space test")

	for i := 0; i < tr.cfg.numWriters; i++ {
		// Create WriterConfigs for them to write to a random set of keys with randomly generated
		// data.
		writerCfg := WriterConfig{
			workerCfg: WorkerConfig{
				Id:       i,
				IMDS:     tr.imds,
				Wg:       tr.cfg.wg,
				KeySpace: tr.cfg.keySpace,
			},
			mode:      tr.cfg.mode,
			sleepTime: tr.cfg.writersSleepTime,
			numWrites: tr.cfg.numWriterWrites,
		}
		writer := NewWriter(writerCfg)
		tr.writers[i] = writer
		writer.Start()
	}
	log.Infof("All writers have been started")

	// Wait on what amounts to the writers to finish writing and then call cancel.
	// That will cause the IMDS to cancel and begin the shutdown process.
	tr.cfg.wg.Wait()
	tr.cfg.cancel()
	tr.readerWg.Wait()
}

func (tr *TestRunner) startReaders() {
	for i := 0; i < tr.cfg.numReaders; i++ {
		readerCfg := ReaderConfig{
			workerCfg: WorkerConfig{
				Id:       i,
				IMDS:     tr.imds,
				Wg:       tr.readerWg,
				KeySpace: tr.cfg.keySpace,
			},
			sleepTime: tr.cfg.readersSleepTime,
			ctx:       tr.cfg.ctx,
		}
		reader := NewReader(readerCfg)
		tr.readers[i] = reader
		reader.Run()
	}
}
