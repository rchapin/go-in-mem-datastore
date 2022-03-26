# Go In-Memory Datastore

## Overview

An in-memory data store that is also persisted to disk.

![alt text for screen readers](overview.png "Go In-Memory Datastore Overview")

Supports concurrent ```Get(key string)``` and ```Put(key string, val map[string]interface{})```.

All reads are directly from an in-memory ```map[string]interface{}``` data store.  The test records are a 100 field Avro record with 50 doubles and 50 strings.  Each record is approximately 102800 bytes.  Since most of the data is strings, in order to easily "bulk" up the test payloads, it does not gain as much benefit from the default avro encoding algorithm.

For each incoming record, the writer first validates that this is the most recent record for the given key in the in-memory store.  Otherwise, it is possible that, even though we would persist the most recent data for a given key to disk, we might return stale data to readers.

All writes are persisted to disk at the time of write.  In order to increase performance, once the write to the in-memory store is complete and the mutex unlocked the incoming data is written to a channel.  That channel is read by multiple ```Serializer``` go routines that each write records to separate files to parallelize I/O operations.

It should be possible to use this as-is in an embedded environment as long as it is connected to a network and can ship the data files to another store for analytics.
## To Dos

1. Shard the in-memory data store.  We can hash the incoming key and then partition all of the data into n number of ```map[string]interface{}``` instances, enabling multiple concurrent writes to both the data in memory and on disk.

1. Implement cache population with existing data on start-up.  Currently, it does not yet load the persisted data from disk on start-up.  The plan is to implement the functionality such that the cache is pre-populated with the most recent record from each key on start-up by reading all of the data in the output directory.

1. Rotate the output files based on size or number of records. The ```Serializers``` do not yet do any size or record based rotation on the output files.

1. Remove the avro serialization in lieu of serializing ```map[string]interface{}``` data as JSON; one JSON record per line.
## Performance

There is nothing particularly complicated about the program and it should not require any special hardware.  The following stats were gleaned from running on the following system:

- Intel(R) Core(TM) i7 CPU 920  @ 2.67GHz
- 24GB RAM
- 1TB WDC WD1003FZEX-0, 7200RPM a quick test of the disk indicates about 2.3GB/s

With the following test:

- 40 writers, each writing 2400 records at 1 record per millisecond
- 40 readers, each reading every 2 milliseconds

Writes per milli=1.663432
Reads per milli=17.232257
Read stats in milliseconds: min=9.3e-05, mean=0.01755602376083348, max=79.856252
Write stats in milliseconds: min=0.000502, mean=0.5762599500411977, max=113.28934
Total reads=994508, writes=96000

I would like to see this data as a histogram to see the percentile distribution but just went with the simple thing first to get an idea of how it would perform.

## Running the Tests

**The tests include a performance test that will require about 3.2GB of RAM and write approximately 10GB to ```/var/tmp/inmemdatastore-inttest``` dir.***

Currently, it will leave the data in the ```/var/tmp/inmemdatastore-inttest``` for analysis.

1. Change dirs into the integration_tests dir and execute the tests.
    ```
    cd integration_tests/ && go test -count=1 -v -tags=integration ./...
    ```