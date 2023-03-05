package inmemdatastore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/linkedin/goavro/v2"
	log "github.com/rchapin/rlog"
)

type Writer interface {
	Write(interface{}) error
	Shutdown()
}

type WriterCfg struct {
	Id               int
	OutputDir        string
	// Instead of creating a file name that is simply the id of the writer and the specific
	// file extension,
	FileNameOverride string
}

func CreateFileName(id int, fileNameExtension, fileNameOverride string) string {
	var retval string
	if fileNameOverride != "" {
		retval = fmt.Sprintf("%s.%s", fileNameOverride, fileNameExtension)
	} else {
		retval = fmt.Sprintf("%d.%s", id, fileNameExtension)
	}
	return retval
}

// Since there is currently one a single implementation of the Writer interface we will just keep
// all of this code in a single file.

const avroFileExt = "avro"

type AvroFileWriter struct {
	ctx            context.Context
	wg             *sync.WaitGroup
	id             int
	outputDir      string
	outputFileName string
	outputFilePath string
	avroSchema     string
	codec          *goavro.Codec
	fh             *os.File
	ocfw           *goavro.OCFWriter
}

type AvroFileWriterConfig struct {
	WriterCfg
	AvroSchema string
}

func NewAvroFileWriter(ctx context.Context, wg *sync.WaitGroup, cfg AvroFileWriterConfig) *AvroFileWriter {
	fileName := CreateFileName(cfg.Id, avroFileExt, cfg.FileNameOverride)
	retval := &AvroFileWriter{
		ctx:            ctx,
		wg:             wg,
		id:             cfg.Id,
		outputDir:      cfg.OutputDir,
		outputFileName: fileName,
		outputFilePath: filepath.Join(cfg.OutputDir, fileName),
		avroSchema:     cfg.AvroSchema,
	}

	codec, err := GetAvroCodec(cfg.AvroSchema)
	if err != nil {
		panic(err)
	}
	retval.codec = codec

	// For the time being we will just initialize our output file on instantiation and ignore that
	// there might be any existing files in the output dir.
	retval.makeFile()

	return retval
}

func (a *AvroFileWriter) Write(data interface{}) error {
	record := data.(map[string]interface{})
	values := []map[string]interface{}{record}
	return a.ocfw.Append(values)
}

func (a *AvroFileWriter) Shutdown() {
	log.Infof("Serializer shutting down; id=%d", a.id)
	err := a.fh.Close()
	if err != nil {
		// FIXME: Refactor this so that we can pass back these errors on a channel.
		log.Error(err)
	}
}

func (a *AvroFileWriter) makeFile() {
	fh, err := os.Create(a.outputFilePath)
	if err != nil {
		// Not sure if there is any other better way to handle this.  If, given the configs provide
		// to generate output file paths, we cannot a file and get a file handle, what else is there
		// that we can do here?
		panic(err)
	}
	a.fh = fh
	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:     fh,
		Codec: a.codec,
	})
	if err != nil {
		panic(err)
	}
	a.ocfw = ocfw
}
