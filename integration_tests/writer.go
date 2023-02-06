package inttest

import (
	"time"

	log "github.com/rchapin/rlog"
)

type WriterConfig struct {
	workerCfg       WorkerConfig
	mode            Mode
	SpecificRecords []map[string]interface{}
	numWrites       int
	sleepTime       int64
	numDblFields    int
	numStrFields    int
}
type Writer struct {
	worker *Worker
	cfg    WriterConfig
}

func NewWriter(cfg WriterConfig) *Writer {
	retval := &Writer{
		worker: NewWorker(cfg.workerCfg),
		cfg:    cfg,
	}
	return retval
}

func (w *Writer) Start() {
	log.Infof("Writer id=%d starting", w.worker.id)
	w.cfg.workerCfg.Wg.Add(1)
	go func() {
		defer w.cfg.workerCfg.Wg.Done()
		// Write out each of the records based on the mode in which we are configured
		switch w.cfg.mode {
		case SpecificRecords:
			w.writeSpecificRecords()
		case LimitedKeySpace:
			w.writeLimitedKeySpaceRecords()
		}
		log.Infof("Writer finished running; id=%d", w.worker.id)
	}()
}

func (w *Writer) writeSpecificRecords() {
	for _, rec := range w.cfg.SpecificRecords {
		log.Infof("id=%s", rec["id"].(string))
		w.worker.imds.Put(rec[avroFieldId].(string), rec)
		time.Sleep(time.Duration(w.cfg.sleepTime) * time.Millisecond)
	}
}

func (w *Writer) writeLimitedKeySpaceRecords() {
	for i := 0; i < w.cfg.numWrites; i++ {
		key := w.worker.getRandomKey()
		log.Debugf("Writer id=%d, writing record with key=%s", w.worker.id, key)
		recSpec := RecordSpec{Id: key, CollectionTime: time.Now().UTC().UnixNano()}
		record := generateAvroRecord(recSpec, w.cfg.numDblFields, w.cfg.numStrFields)
		w.worker.imds.Put(key, record)
		time.Sleep(time.Duration(w.cfg.sleepTime) * time.Millisecond)
	}
}
