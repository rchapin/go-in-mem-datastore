//go:build integration

package inttest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/linkedin/goavro"
)

type ResourceManager struct {
	// Context and cancelfunc for the test code itself
	tCtx    context.Context
	tCancel context.CancelFunc
	// Context and cancelfunc to be passed into the test runner and the OUT
	appCtx                 context.Context
	appCancel              context.CancelFunc
	wg                     *sync.WaitGroup
	codec                  *goavro.Codec
	avroString             string
	avroJson               map[string]interface{}
	avroNumMetricDblFields int
	avroNumMetricStrFields int
	testParentDirPath      string
	testDataDirPath        string
	testDirs               map[string]string
}

func NewResourceManager(testParentDir string) *ResourceManager {
	ctx, cancel := context.WithCancel(context.Background())
	retval := &ResourceManager{
		tCtx:              ctx,
		tCancel:           cancel,
		testParentDirPath: testParentDir,
	}

	// Concatenate the paths for all of the test dirs that we will create under our parent test
	// directory.
	testDirs := map[string]string{}
	testDirs[dirData] = filepath.Join(testParentDir, dirData)
	retval.testDataDirPath = testDirs[dirData]
	retval.testDirs = testDirs

	// Load our avro definition file from disk and then generate both a "JSON"
	// map[string]interface{} and a goavro.Codec from it.  We will use the JSON representation of
	// the schema to glean information about the names and numbers of the fields it contains.
	retval.avroString = readFile(fmt.Sprintf("testdata/%s", avroSchemaFile))

	var avroJson map[string]interface{}
	err := json.Unmarshal([]byte(retval.avroString), &avroJson)
	if err != nil {
		panic(err)
	}
	retval.avroJson = avroJson

	// Determine how many "metricdbl" and "metricstr" fields are contained in the avro schema.  We
	// need this so that we can later dynamically generate avro data.
	retval.avroNumMetricDblFields, retval.avroNumMetricStrFields = getFieldTypeCounts(avroJson)

	codec, err := goavro.NewCodec(retval.avroString)
	if err != nil {
		panic(err)
	}
	retval.codec = codec
	return retval
}

func getFieldTypeCounts(avroJson map[string]interface{}) (int, int) {
	fields, ok := avroJson["fields"].([]interface{})
	if !ok {
		panic(fmt.Errorf("unable to extract fields field from avroJson"))
	}
	var numMetricDblFields int
	var numMetricStrFields int
	for _, field := range fields {
		fieldMap := field.(map[string]interface{})
		fieldName := fieldMap["name"].(string)
		if strings.Contains(fieldName, metricDblPrefix) {
			numMetricDblFields++
			continue
		}
		if strings.Contains(fieldName, metricStrPrefix) {
			numMetricStrFields++
		}
	}
	return numMetricDblFields, numMetricStrFields
}

func (r *ResourceManager) refreshContextsWg() {
	// For each test we need to create a new set of context, cancelfunc and waitgroups to pass into
	// the TestRunner.  We generate the "sub" context/cancelfunc from the top level context that we
	// use to control the test suite itself.
	ctx, cancel := context.WithCancel(r.tCtx)
	r.appCtx = ctx
	r.appCancel = cancel
	r.wg = &sync.WaitGroup{}
}

func (r *ResourceManager) setupTestDirs() {
	err := os.MkdirAll(r.testParentDirPath, 0o755)
	if err != nil {
		panic(err)
	}
	for _, d := range r.testDirs {
		err := os.RemoveAll(d)
		if err != nil {
			panic(err)
		}
		os.Mkdir(d, 0o755)
	}
}
