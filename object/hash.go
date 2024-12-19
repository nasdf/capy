package object

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
