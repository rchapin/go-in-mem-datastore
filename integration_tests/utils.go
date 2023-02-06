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

	"github.com/linkedin/goavro"
	log "github.com/rchapin/rlog"
)

type RecordSpec struct {
	Id             string
	CollectionTime int64
}

type TestConfigs struct {
	PerfNumReaders   int
	PerfWriters      int
	PerfWriterWrites int
}

var (
	maxFloat = float64(1 << 64)
	minFloat = float64(0.01)
	maxInt   = int(1 << 62)
	minInt   = int(1)
)

func generateAvroRecord(recSpec RecordSpec, numDblFields, numStrFields int) map[string]interface{} {
	retval := map[string]interface{}{}
	// retval[avroFieldId] = fmt.Sprintf("%s%d", idPrefix, recordSpec)
	retval[avroFieldId] = recSpec.Id
	retval[avroFieldCollectionTime] = recSpec.CollectionTime

	// Dynamically generate N number of values for all of the metricdbl fields in the schema.
	for j := 1; j <= numDblFields; j++ {
		retval[fmt.Sprintf("%s%d", metricDblPrefix, j)] = getRandomFloat()
	}
	// Dynamically generate N number of values for all of the metricstr fields in the schema.
	for j := 1; j <= numStrFields; j++ {
		retval[fmt.Sprintf("%s%d", metricStrPrefix, j)] = getRandomString()
	}

	return retval
}

// func generateAvroRecords(numRecords, numDblFields, numStrFields int) []map[string]interface{} {
// 	retval := []map[string]interface{}{}
// 	for i := 0; i < numRecords; i++ {
// 		recSpec := RecordSpec{
// 			Id:             fmt.Sprintf("%s%d", idPrefix, i),
// 			CollectionTime: time.Now().UnixNano(),
// 		}
// 		retval = append(retval, generateAvroRecord(recSpec, numDblFields, numStrFields))
// 	}
// 	return retval
// }

func generateRecordsFromRecordSpecs(recSpecs []RecordSpec, numDblFields, numStrFields int) []map[string]interface{} {
	retval := []map[string]interface{}{}
	for _, recSpec := range recSpecs {
		retval = append(retval, generateAvroRecord(recSpec, numDblFields, numStrFields))
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
func loadAllAvroRecords(path string, justCounts bool) ([]map[string]interface{}, int64) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		panic(err)
	}
	data := []map[string]interface{}{}
	var count int64
	for _, file := range files {
		recs, curCount := loadAvroRecords(filepath.Join(path, file.Name()), justCounts)
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

// func writeTestAvroFile(path string, records []map[string]interface{}) {
// 	// FIXME: pass in file name
// 	filePath := filepath.Join(path, "test.avro")
// 	file, err := os.Create(filePath)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer file.Close()
// 	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
// 		W:     file,
// 		Codec: rm.codec,
// 	})
// 	err = ocfw.Append(records)
// 	if err != nil {
// 		panic(err)
// 	}
// }
