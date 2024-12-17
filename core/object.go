package core

import (
	"bytes"
	"encoding/hex"

	"github.com/fxamacker/cbor/v2"
	"golang.org/x/crypto/sha3"
)

// Hash is the unique hash of an object.
type Hash []byte

// Sum returns the hash of the given data.
func Sum(data []byte) Hash {
	hash := sha3.Sum256(data)
	return Hash(hash[:])
}

// Equal returns true if the given hash is equal to this hash.
func (h Hash) Equal(other Hash) bool {
	return bytes.Equal(h, other)
}

// String returns the hex representation of the hash.
func (h Hash) String() string {
	return hex.EncodeToString(h)
}

type Object interface {
	Encode() ([]byte, error)
}

// Commit contains the state of the collections at point in time.
type Commit struct {
	// Parents is the list of parent commits this commit was created from.
	Parents []Hash
	// DataRoot is the hash of the data root.
	DataRoot Hash
}

func DecodeCommit(data []byte) (*Commit, error) {
	var commit Commit
	if err := cbor.Unmarshal(data, &commit); err != nil {
		return nil, err
	}
	return &commit, nil
}

func (c Commit) Encode() ([]byte, error) {
	enc, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	return enc.Marshal(c)
}

// DataRoot is the root object for all data.
type DataRoot struct {
	// Collections is a mapping of names to collection root hashes.
	Collections map[string]Hash
}

func DecodeDataRoot(data []byte) (*DataRoot, error) {
	var dataRoot DataRoot
	if err := cbor.Unmarshal(data, &dataRoot); err != nil {
		return nil, err
	}
	return &dataRoot, nil
}

func (d DataRoot) Encode() ([]byte, error) {
	enc, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	return enc.Marshal(d)
}

// Collection is the root object for a collection.
type Collection struct {
	// Documents is a mapping of ids to document hashes.
	Documents map[string]Hash
}

func DecodeCollection(data []byte) (*Collection, error) {
	var collection Collection
	if err := cbor.Unmarshal(data, &collection); err != nil {
		return nil, err
	}
	return &collection, nil
}

func (c Collection) Encode() ([]byte, error) {
	enc, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	return enc.Marshal(c)
}

type Document map[string]any

func NewDocument() Document {
	return make(map[string]any)
}

func DecodeDocument(data []byte) (Document, error) {
	var document Document
	if err := cbor.Unmarshal(data, &document); err != nil {
		return nil, err
	}
	return document, nil
}

func (d Document) Encode() ([]byte, error) {
	enc, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	return enc.Marshal(d)
}
