package inmemdatastore

import "github.com/linkedin/goavro/v2"

func GetAvroCodec(schema string) (*goavro.Codec, error) {
	retval, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}
	return retval, nil
}
