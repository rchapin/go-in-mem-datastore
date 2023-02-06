package inttest

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/rchapin/go-in-mem-datastore/inmemdatastore"
	"github.com/rchapin/go-in-mem-datastore/utils"
	log "github.com/rchapin/rlog"
	"github.com/stretchr/testify/assert"
)

var rm *ResourceManager

// Setup and Teardown functions ------------------------------------------------

func setupSignalHandler(cancel context.CancelFunc) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		signal := <-c
		fmt.Printf("Received shutdown signal; signal=%+v\n", signal)
		cancel()
		// os.Exit(0)
	}()
}

func setUpTest() {
	rm = NewResourceManager(testParentDir)
}

func setUpSubTest() {
	rm.setupTestDirs()
	rm.refreshContextsWg()
}

func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION") == "" {
		fmt.Printf("Skipping integration tests.  To run set env var INTEGRATION=1")
		return
	}

	utils.SetupLogging("info")
	log.Info("From integration test TestMain")
	setUpTest()
	setupSignalHandler(rm.tCancel)
	runExitCode := m.Run()
	log.Info("Integration tests complete")
	os.Exit(runExitCode)
}

// Integration Tests -----------------------------------------------------------

// TestWriteRecords tests that the IMDS will only write records to the cache that are newer than the
// record already there for the same key, but that it will persist all of the records to the files
// on disk.
func TestWriteRecords(t *testing.T) {
	utils.SetupLogging("debug")
	setUpSubTest()
	// Define two sets of specific records in a specific order that we want the Writers to use for
	// the test.  Both keys will include a record that is written with an older timestamp than the
	// one before it.  This ensures that the IMDS is only writing records in the cache that are the
	// most recent records.
	startTimestamp := int64(1647106627392928613)
	record1A := RecordSpec{
		Id:             "sensor101",
		CollectionTime: startTimestamp,
	}
	// Another record from the same sensor (same key) generated AFTER the first.  This is the
	// record we expect to be in the cache.
	record1B := RecordSpec{
		Id:             "sensor101",
		CollectionTime: startTimestamp + 100,
	}
	// This record is one with a timestamp BEFORE the last one.  We expect that this record
	// should be persisted to disk but NOT be in the cache.
	record1C := RecordSpec{
		Id:             "sensor101",
		CollectionTime: startTimestamp + 50,
	}
	writer1RecSpecs := []RecordSpec{record1A, record1B, record1C}

	record2A := RecordSpec{
		Id:             "sensor201",
		CollectionTime: startTimestamp,
	}
	// A record with a timestamp BEFORE the last one.
	record2B := RecordSpec{
		Id:             "sensor201",
		CollectionTime: startTimestamp - 100,
	}
	// We expect this to be the record in the cache after writing all of the records.
	record2C := RecordSpec{
		Id:             "sensor201",
		CollectionTime: startTimestamp + 110,
	}
	writer2RecSpecs := []RecordSpec{record2A, record2B, record2C}
	writer1Records := generateRecordsFromRecordSpecs(writer1RecSpecs, rm.avroNumMetricDblFields, rm.avroNumMetricStrFields)
	writer2Records := generateRecordsFromRecordSpecs(writer2RecSpecs, rm.avroNumMetricDblFields, rm.avroNumMetricStrFields)

	// Generate a map that is keyed by the writer id and includes the records that we are
	// programming each writer to write to the IMDS.
	testRecords := make(map[int][]map[string]interface{})
	testRecords[0] = writer1Records
	testRecords[1] = writer2Records

	// The keys that we expect to be written to the datastore and that we will execute queries on.
	keySpace := []string{"sensor101", "sensor201"}

	// Generate the expected set of records that we expect to find in both the cache and persisted to disk.
	expectedPersistedRecSpecs := append(writer1RecSpecs, writer2RecSpecs...)
	expectedCachedRecSpecs := []RecordSpec{
		record1B,
		record2C,
	}

	// Build a configuration for our test runner to be able to execute the test
	trCfg := TRConfig{
		mode:                    SpecificRecords,
		specificRecords:         testRecords,
		persistenceChanBuffSize: 1024,
		testWriterSleepTime:     2,
		numPersisters:           2,
		numDatastoreShards:      2,
		numTestReaders:          20,
		testReaderSleepTime:     5,
		schema:                  rm.avroSchemaString,
		outputDirPath:           rm.testDirs[dirData],
		keySpace:                keySpace,
		numDblFields:            rm.avroNumMetricDblFields,
		numStrFields:            rm.avroNumMetricStrFields,
	}
	tr := NewTestRunner(rm.testRunnerCtx, rm.testRunnerCancel, rm.testRunnerWg, trCfg)
	tr.RunTest()

	validatePersistedData(t, tr.imds, expectedPersistedRecSpecs)
	validateCachedData(t, tr.imds, expectedCachedRecSpecs)
	validateShardKeys(t, tr.imds)
	printStats(tr)
}

func TestPerformance(t *testing.T) {
	utils.SetupLogging("info")
	setUpSubTest()

	// For this test we will generate a set key space for the records that we will generate and for
	// which we will run queries.
	keySpace := make([]string, rm.perfConfig.numTestKeys)
	for i := 0; i < rm.perfConfig.numTestKeys; i++ {
		keySpace[i] = fmt.Sprintf("%s%d", idPrefix, i)
	}

	// Build a configuration for our test runner to be able to execute the test
	trCfg := TRConfig{
		mode:                    LimitedKeySpace,
		persistenceChanBuffSize: rm.perfConfig.serializerChanBuffSize,
		numPersisters:           rm.perfConfig.numSerializers,
		numTestReaders:          rm.perfConfig.numReaders,
		numDatastoreShards:      rm.perfConfig.datastoreShards,
		testReaderSleepTime:     rm.perfConfig.readerSleepTime,
		numTestWriters:          rm.perfConfig.numWriters,
		numTestWriterWrites:     rm.perfConfig.numWritesPerWriter,
		testWriterSleepTime:     rm.perfConfig.writerSleepTime,
		schema:                  rm.avroSchemaString,
		outputDirPath:           rm.testDirs[dirData],
		keySpace:                keySpace,
		numDblFields:            rm.avroNumMetricDblFields,
		numStrFields:            rm.avroNumMetricStrFields,
	}
	tr := NewTestRunner(rm.testRunnerCtx, rm.testRunnerCancel, rm.testRunnerWg, trCfg)
	tr.RunTest()

	_, recCounts := loadAllAvroRecords(rm.testDirs[dirData], true)
	expectedWrites := int64(rm.perfConfig.numWriters * rm.perfConfig.numWritesPerWriter)
	assert.Equal(t, expectedWrites, recCounts)

	validateShardKeys(t, tr.imds)
	printStats(tr)
}

func printStats(tr *TestRunner) {
	// Aggregate the total number of reads and writes across all of the Datastore instances.
	var totalReads int64
	var totalWrites int64
	for _, datastore := range tr.imds.GetDatastores() {
		totalReads += datastore.NumReads
		totalWrites += datastore.NumWrites
	}

	duration := tr.EndTime - tr.StartTime
	writeRate := float64(totalWrites) / float64(duration)
	readRate := float64(totalReads) / float64(duration)
	log.Infof("Writes/s=%f, Reads/s=%f", writeRate, readRate)
}

func validatePersistedData(t *testing.T, IMDS *inmemdatastore.InMemDataStore, expectedRecSpecs []RecordSpec) {
	actualRecords, _ := loadAllAvroRecords(rm.testDirs[dirData], false)
	assert.Equal(t, len(expectedRecSpecs), len(actualRecords))

	// Now we need to build a "set" that contains a key for each of the records that we expect to
	// have been persisted to disk. In this case we will concatenate the id and collection_end time
	// for a composite key.  We will then iterate over all of the actual records and for each that
	// we find, remove the key from the set.  Ultimately, we should have an empty set when we are
	// finished.
	keySet := getKeySetFromExpectedRecSpecs(expectedRecSpecs)
	for _, ar := range actualRecords {
		key := fmt.Sprintf("%s_%d", ar[avroFieldId], ar[avroFieldCollectionTime])
		checkForKey(t, keySet, key)
	}
	assert.True(t, len(keySet) == 0)
}

func validateCachedData(t *testing.T, IMDS *inmemdatastore.InMemDataStore, recSpecs []RecordSpec) {
	// Validate that the cache contains all of the expected records
	datastore := IMDS.GetAll()
	assert.Equal(t, len(recSpecs), len(datastore))
	keySet := getKeySetFromExpectedRecSpecs(recSpecs)
	for k, v := range datastore {
		record := v.(map[string]interface{})
		key := fmt.Sprintf("%s_%d", k, record[avroFieldCollectionTime])
		checkForKey(t, keySet, key)
	}
	assert.True(t, len(keySet) == 0)
}

func validateShardKeys(t *testing.T, IMDS *inmemdatastore.InMemDataStore) {
	// Validate that each of the shards in the datastore contain a unique set of keys
	datastores := IMDS.GetDatastores()
	dsKeysByDatastoreId := make(map[uint64]map[string]bool, len(datastores))
	for datastoreId, datastore := range datastores {
		data := datastore.Data
		for k := range data {
			dsKeys, ok := dsKeysByDatastoreId[datastoreId]
			if !ok {
				dsKeys = make(map[string]bool)
				dsKeysByDatastoreId[datastoreId] = dsKeys
			}
			dsKeys[k] = true
		}
	}

	numKeys := uint64(len(dsKeysByDatastoreId))
	for i := uint64(0); i < numKeys; i++ {
		// If this is the last set of keys in the dsKeysByDatastoreId map and we have not
		// encountered an assertion failure, we can break.  We have already checked all of the
		// other maps against this one and not found any duplicate keys.
		if i+1 == numKeys {
			break
		}
		currMap := dsKeysByDatastoreId[i]

		// For each of the remaining sets of keys in the dsKeysByDatastoreId map, we need to check
		// that none of the keys in the current map are present.
		for j := i + 1; j < numKeys; j++ {
			nextMap := dsKeysByDatastoreId[j]
			for currKey := range currMap {
				for nextMapKey := range nextMap {
					assert.NotEqualValues(t, currKey, nextMapKey)
				}
			}
		}
		delete(dsKeysByDatastoreId, i)
	}
}

func checkForKey(t *testing.T, keySet map[string]bool, actualKey string) {
	_, ok := keySet[actualKey]
	if !ok {
		assert.Failf(t, "We did not find a record with the key=%s in the expected results", actualKey)
	} else {
		delete(keySet, actualKey)
	}
}
