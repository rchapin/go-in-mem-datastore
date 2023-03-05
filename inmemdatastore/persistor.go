package inmemdatastore

import (
	"context"
	"sync"

	log "github.com/rchapin/rlog"
)

type PersisterConfig struct {
	Id            int
	Serializer    Serializer
	Writer        Writer
	InputChan     PersistenceChan
	PersisterType PersisterType
}

type Persister struct {
	ctx       context.Context
	wg        *sync.WaitGroup
	id        int
	inputChan PersistenceChan
	Serializer
	Writer
}

func NewPersister(ctx context.Context, wg *sync.WaitGroup, cfg PersisterConfig) *Persister {
	return &Persister{
		ctx:        ctx,
		wg:         wg,
		id:         cfg.Id,
		Serializer: cfg.Serializer,
		Writer:     cfg.Writer,
		inputChan:  cfg.InputChan,
	}
}

func (p *Persister) Run() {
	log.Infof("Persister starting Run, id=%d", p.id)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case record := <-p.inputChan:
				err := p.persist(record)
				if err != nil {
					// FIXME: Do something other than panicking here
					panic(err)
				}
			case <-p.ctx.Done():
				log.Infof("Persister exiting Run loop on context done; id=%d ", p.id)
				// We must ensure that the channel is empty before we shutdown or we can leave data
				// on it that never gets persisted to disk
				for {
					if len(p.inputChan) > 0 {
						record := <-p.inputChan
						err := p.persist(record)
						if err != nil {
							// FIXME: Do something other than panicking here
							panic(err)
						}
					} else {
						break
					}
				}
				p.Shutdown()
				return
			}
		}
	}()
}

func (p *Persister) persist(record map[string]interface{}) error {
	data, err := p.Serialize(record)
	if err != nil {
		return err
	}
	err = p.Write(data)
	if err != nil {
		return err
	}
	return nil
}
