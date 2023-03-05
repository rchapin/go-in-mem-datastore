package inttest

const (
	avroSchemaFile          = "metrics.avsc"
	avroFieldId             = "id"
	avroFieldCollectionTime = "collection_time"
	idPrefix                = "sensor"
	metricDblPrefix         = "metricdbl"
	metricStrPrefix         = "metricstr"
	testParentDir           = "/var/tmp/inmemdatastore-inttest"
	recordIdKey             = "id"
	recordTimestampKey      = "collection_time"
	dirData                 = "data"
	dirCache                = "cache"
	// How many times are we going to concatenate the hex value that we generate from a random
	// number in our integration_test.getRandomString() function
	randomStringGenIterations = 16

	envVarIntegrationTest             = "INTEGRATION"
	envVarPerfNumTestKeys             = "IMDS_INTTEST_PERF_NUM_TEST_KEYS"
	perfNumTestKeysDefault            = 200
	envVarPerfNumReaders              = "IMDS_INTTEST_PERF_NUM_READERS"
	perfNumReadersDefault             = 40
	envVarPerfNumWriters              = "IMDS_INTTEST_PERF_NUM_WRITERS"
	perfNumWritersDefault             = 40
	envVarPerfNumWritesPerWriter      = "IMDS_INTTEST_PERF_NUM_WRITES_PER_WRITER"
	perfNumWritesPerWriterDefault     = 2400
	envVarPerfNumSerializers          = "IMDS_INTTEST_PERF_NUM_SERIALIZERS"
	perfNumSerializersDefault         = 16
	envVarPerfSerializerChanBuffSize  = "IMDS_INTTEST_PERF_SERIALIZER_CHAN_BUFF_SIZE"
	perfSerializerChanBuffSizeDefault = 2048
	envVarPerfDatastoreShards         = "IMDS_INTTEST_PERF_DATASTORE_SHARDS"
	perfDatastoreShardsDefault        = 8
	envVarPerfReaderSleepTime         = "IMDS_INTTEST_PERF_READER_SLEEP_TIME"
	perfReadersSleepTimeDefault       = 2
	envVarPerfWriterSleepTime         = "IMDS_INTTEST_PERF_WRITES_SLEEP_TIME"
	perfWriterSleepTimeDefault        = 1
)
