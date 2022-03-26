package inmemdatastore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/linkedin/goavro"

	log "github.com/rchapin/rlog"
)

type SerializerConfig struct {
	ctx       context.Context
	id        int
	outputDir string
	codec     *goavro.Codec
	inputChan SerializationChan
	wg        *sync.WaitGroup
}
type Serializer struct {
	cfg            SerializerConfig
	outputFileName string
	outputFilePath string
	fh             *os.File
	ocfw           *goavro.OCFWriter
	inputChan      SerializationChan
}

// func NewSerializer(id int, outputDir string, codec *goavro.Codec) *Serializer {
func NewSerializer(cfg SerializerConfig) *Serializer {
	fileName := fmt.Sprintf("%d.avro", cfg.id)
	retval := &Serializer{
		cfg:            cfg,
		outputFileName: fileName,
		outputFilePath: filepath.Join(cfg.outputDir, fileName),
		inputChan:      cfg.inputChan,
	}

	// For the time being we will just initialize our output file on instantiation and ignore that
	// there might be any existing files in the output dir.
	retval.makeFile()

	return retval
}

func (s *Serializer) makeFile() {
	fh, err := os.Create(s.outputFilePath)
	if err != nil {
		// TODO: handle this error properly
		panic(err)
	}
	s.fh = fh

	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:     fh,
		Codec: s.cfg.codec,
	})
	if err != nil {
		// TODO: handle this error properly
		panic(err)
	}
	s.ocfw = ocfw
}

func (s *Serializer) Run() {
	log.Infof("Serializer id=%d starting Run", s.cfg.id)
	s.cfg.wg.Add(1)
	go func() {
		defer s.cfg.wg.Done()
		for {
			select {
			case record := <-s.inputChan:
				log.Debugf(
					"Serializer id=%d received record from channel, id=%s, collection_time=%d",
					s.cfg.id,
					record[RecFieldId],
					record[RecFieldCollectionTime])
				s.writeRecord(record)
			case <-s.cfg.ctx.Done():
				log.Infof("Serializer id=%d exiting Run loop on context done", s.cfg.id)

				// We must ensure that the channel is empty before we shutdown or we can leave data
				// on it that never gets persisted to disk
				for {
					if len(s.inputChan) > 0 {
						record := <-s.inputChan
						s.writeRecord(record)
					} else {
						break
					}
				}
				s.shutdown()
				return
			}
		}
	}()
}

func (s *Serializer) shutdown() {
	log.Infof("Serializer id=%d shutting down", s.cfg.id)
	err := s.fh.Close()
	if err != nil {
		// FIXME: Refactor this so that we can pass back these errors on a channel.
		log.Error(err)
	}
}

func (s *Serializer) writeRecord(record map[string]interface{}) {
	values := []map[string]interface{}{record}
	if err := s.ocfw.Append(values); err != nil {
		// TODO: better sort out how we handle errors.  They should probably be published to a
		// channel so that the composing module can better handle a write error.
		panic(err)
	}
}
