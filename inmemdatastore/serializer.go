package inmemdatastore

import "github.com/linkedin/goavro/v2"

type Serializer interface {
	Serialize(map[string]interface{}) (interface{}, error)
}

type NoopSerializer struct{}

func NewNoopSerializer() Serializer {
	return &NoopSerializer{}
}

func (n *NoopSerializer) Serialize(record map[string]interface{}) (interface{}, error) {
	return record, nil
}

type AvroSerializer struct {
	avroSchema string
	codec      *goavro.Codec
}

func NewAvroSerializer(avroSchema string) Serializer {
	codec, err := GetAvroCodec(avroSchema)
	if err != nil {
		panic(err)
	}
	retval := &AvroSerializer{
		avroSchema: avroSchema,
		codec:      codec,
	}
	return retval
}

func (a *AvroSerializer) Serialize(record map[string]interface{}) (interface{}, error) {
	return a.codec.BinaryFromNative(nil, record)
}
