package core

import (
	"bytes"
	"encoding/hex"

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

// Commit contains the state of the collections at point in time.
type Commit struct {
	// Parents is the list of parent commits this commit was created from.
	Parents []Hash
	// DataRoot is the hash of the data root.
	DataRoot Hash
}

// DataRoot is the root object for all data.
type DataRoot struct {
	// Collections is a mapping of names to collection root hashes.
	Collections map[string]Hash
}

// CollectionRoot is the root object for a collection.
type CollectionRoot struct {
	// Documents is a mapping of ids to document hashes.
	Documents map[string]Hash
}
