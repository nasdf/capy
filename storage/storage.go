package storage

import (
	"errors"

	"github.com/ipld/go-ipld-prime/storage"
)

var ErrNotFound = errors.New("key not found")

type Storage interface {
	storage.ReadableStorage
	storage.WritableStorage
}
