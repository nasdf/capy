package core

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/rodent-software/capy/codec"
	"github.com/rodent-software/capy/object"
	"golang.org/x/crypto/sha3"
)

var ErrNotFound = errors.New("key not found")

type Storage interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) error
}

// EncodeObject writes an encoded object to the given storage and returns its hash.
//
// The key for the object is the computed hash of the encoded bytes.
func EncodeObject(ctx context.Context, storage Storage, value any) (object.Hash, error) {
	hash := sha3.New256()
	buff := bytes.NewBuffer(nil)

	enc := codec.NewEncoder(io.MultiWriter(hash, buff))
	err := enc.Encode(value)
	if err != nil {
		return nil, err
	}
	err = enc.Flush()
	if err != nil {
		return nil, err
	}

	sum := object.Hash(hash.Sum(nil))
	err = storage.Put(ctx, sum.String(), buff.Bytes())
	if err != nil {
		return nil, err
	}
	return sum, nil
}
