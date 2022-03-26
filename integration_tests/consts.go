//go:build integration

package inttest

const (
	avroSchemaFile          = "metrics.avsc"
	avroFieldId             = "id"
	avroFieldCollectionTime = "collection_time"
	idPrefix                = "sensor"
	metricDblPrefix         = "metricdbl"
	metricStrPrefix         = "metricstr"
	testParentDir           = "/var/tmp/inmemdatastore-inttest"
	recSpecId               = "id"
	recSpecCollectionTime   = "collection_time"
	dirData                 = "data"

	// How many times are we going to concatenate the hex value that we generate from a random
	// number in our integration_test.getRandomString() function
	randomStringGenIterations = 16
)
