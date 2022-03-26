//go:build integration

package inttest

import (
	"bufio"
	"crypto/sha512"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linkedin/goavro"
	"github.com/rchapin/go-in-mem-datastore/inmemdatastore"
	"github.com/rchapin/go-in-mem-datastore/utils"
	log "github.com/rchapin/rlog"
	"github.com/stretchr/testify/assert"
)

type RecordSpec struct {
	Id             string
	CollectionTime int64
}

var (
	maxFloat = float64(1 << 64)
	minFloat = float64(0.01)
	maxInt   = int(1 << 62)
	minInt   = int(1)
)

var rm *ResourceManager

// Utility functions -----------------------------------------------------------

func generateAvroRecord(recSpec RecordSpec) map[string]interface{} {
	retval := map[string]interface{}{}
	// retval[avroFieldId] = fmt.Sprintf("%s%d", idPrefix, recordSpec)
	retval[avroFieldId] = recSpec.Id
	retval[avroFieldCollectionTime] = recSpec.CollectionTime

	// Dynamically generate N number of values for all of the metricdbl fields in the schema.
	for j := 1; j <= rm.avroNumMetricDblFields; j++ {
		retval[fmt.Sprintf("%s%d", metricDblPrefix, j)] = getRandomFloat()
	}
	// Dynamically generate N number of values for all of the metricstr fields in the schema.
	for j := 1; j <= rm.avroNumMetricStrFields; j++ {
		retval[fmt.Sprintf("%s%d", metricStrPrefix, j)] = getRandomString()
	}

	return retval
}

func generateAvroRecords(numRecords int) []map[string]interface{} {
	retval := []map[string]interface{}{}
	for i := 0; i < numRecords; i++ {
		recSpec := RecordSpec{
			Id:             fmt.Sprintf("%s%d", idPrefix, i),
			CollectionTime: time.Now().UnixNano(),
		}
		retval = append(retval, generateAvroRecord(recSpec))
	}
	return retval
}

func generateRecordsFromRecordSpecs(recSpecs []RecordSpec) []map[string]interface{} {
	retval := []map[string]interface{}{}
	for _, recSpec := range recSpecs {
		retval = append(retval, generateAvroRecord(recSpec))
	}
	return retval
}

func getKeySetFromExpectedRecSpecs(recSpecs []RecordSpec) map[string]bool {
	// Generate a set of keys, composed of the id and collection_time values.
	keySet := map[string]bool{}
	for _, r := range recSpecs {
		key := fmt.Sprintf("%s_%d", r.Id, r.CollectionTime)
		keySet[key] = true
	}
	return keySet
}

func getRandomFloat() float64 {
	return minFloat + rand.Float64()*(maxFloat-minFloat)
}

func getRandomInt(min, max int) int {
	// return int(min) + rand.Int()*(max-min)
	return rand.Intn(max-min) + min
}

func getRandomString() string {
	num := getRandomInt(minInt, maxInt)
	data := []byte(strconv.Itoa(num))
	hash := sha512.Sum512(data)
	str := fmt.Sprintf("%x", hash[:])

	// Because we just need some data to bulk up our avro record we'll concatenate our hex value
	// with itself.
	var sb strings.Builder
	for i := 0; i < randomStringGenIterations; i++ {
		sb.WriteString(str)
	}

	return sb.String()
}

// loadAllAvroRecords will load all of the avro records from the data output dir and returns the
// data as well as the count of all of the records found.
func loadAllAvroRecords(justCounts bool) ([]map[string]interface{}, int64) {
	files, err := ioutil.ReadDir(rm.testDirs[dirData])
	if err != nil {
		panic(err)
	}
	data := []map[string]interface{}{}
	var count int64
	for _, file := range files {
		recs, curCount := loadAvroRecords(filepath.Join(rm.testDirs[dirData], file.Name()), justCounts)
		count += curCount
		if !justCounts {
			data = append(data, recs...)
		}
		log.Infof("Loaded %d total avro records", count)
	}
	return data, count
}

func loadAvroRecords(filepath string, justCounts bool) ([]map[string]interface{}, int64) {
	data := []map[string]interface{}{}
	count := int64(0)
	fh, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer fh.Close()
	br := bufio.NewReader(fh)
	ocfr, err := goavro.NewOCFReader(br)
	if err != nil {
		panic(err)
	}
	for ocfr.Scan() {
		datum, err := ocfr.Read()
		if err != nil {
			panic(err)
		}
		count++
		if !justCounts {
			m := datum.(map[string]interface{})
			data = append(data, m)
		}
		if count%2000 == 0 {
			log.Infof("Loaded %d avro records from file=%s", count, filepath)
		}
	}
	return data, count
}

func readFile(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func writeTestAvroFile(records []map[string]interface{}) {
	// FIXME: pass in file name
	filePath := filepath.Join(rm.testParentDirPath, "test.avro")
	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:     file,
		Codec: rm.codec,
	})
	err = ocfw.Append(records)
	if err != nil {
		panic(err)
	}
}

// Setup and Teardown functions ------------------------------------------------

func setUpTest() {
	rm = NewResourceManager(testParentDir)
}

func setUpSubTest() {
	rm.setupTestDirs()
	rm.refreshContextsWg()
}

func TestMain(m *testing.M) {
	utils.SetupLogging("info")

	// Set up a random seed a single time for all of the tests
	rand.Seed(time.Now().UTC().UnixNano())

	log.Info("From integration test TestMain")
	setUpTest()
	// setupSignalHandler(rm.ctxTest, rm.cancelTest)
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

	writer1Records := generateRecordsFromRecordSpecs(writer1RecSpecs)
	writer2Records := generateRecordsFromRecordSpecs(writer2RecSpecs)

	// Generate a map that is keyed by the writer id and includes the records that are programming each writer to
	// write to the IMDS.
	testRecords := map[int][]map[string]interface{}{}
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
		ctx:                      rm.appCtx,
		cancel:                   rm.appCancel,
		mode:                     SpecificRecords,
		specificRecords:          testRecords,
		serializationChanBufSize: 1024,
		writersSleepTime:         2,
		numSerializers:           2,
		numReaders:               20,
		readersSleepTime:         5,
		schema:                   rm.avroString,
		wg:                       rm.wg,
		outputDirPath:            rm.testDirs[dirData],
		keySpace:                 keySpace,
	}
	tr := NewTestRunner(trCfg)
	tr.RunTest()
	rm.wg.Wait()
	log.Info("TestRunner complete")
	tr.imds.Shutdown()

	validatePersistedData(t, tr.imds, expectedPersistedRecSpecs)
	validateCachedData(t, tr.imds, expectedCachedRecSpecs)
}

func TestPerformance(t *testing.T) {
	utils.SetupLogging("info")
	setUpSubTest()

	// For this test we will generate a set key space for the records that we will generate and for
	// which we will run queries.  We will generate 100 different keys.
	numKeys := 200
	keySpace := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keySpace[i] = fmt.Sprintf("%s%d", idPrefix, i)
	}

	numWriters := 40
	numWriterWrites := 2400
	// Build a configuration for our test runner to be able to execute the test
	trCfg := TRConfig{
		ctx:                      rm.appCtx,
		cancel:                   rm.appCancel,
		mode:                     LimitedKeySpace,
		serializationChanBufSize: 2048,
		numSerializers:           16,
		numReaders:               40,
		readersSleepTime:         2,
		numWriters:               numWriters,
		numWriterWrites:          numWriterWrites,
		writersSleepTime:         1,
		schema:                   rm.avroString,
		wg:                       rm.wg,
		outputDirPath:            rm.testDirs[dirData],
		keySpace:                 keySpace,
	}
	tr := NewTestRunner(trCfg)
	tr.RunTest()
	rm.wg.Wait()
	log.Info("TestRunner complete")
	tr.imds.Shutdown()

	_, recCounts := loadAllAvroRecords(true)
	assert.Equal(t, int64(numWriters*numWriterWrites), recCounts)
}

func validatePersistedData(t *testing.T, IMDS *inmemdatastore.InMemDataStore, expectedRecSpecs []RecordSpec) {
	actualRecords, _ := loadAllAvroRecords(false)
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

func checkForKey(t *testing.T, keySet map[string]bool, actualKey string) {
	_, ok := keySet[actualKey]
	if !ok {
		assert.Failf(t, "We did not find a record with the key=%s in the expected results", actualKey)
	} else {
		delete(keySet, actualKey)
	}
}
