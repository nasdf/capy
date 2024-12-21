package codec

import (
	"bufio"
	"fmt"
	"io"
	"math"

	"github.com/rodent-software/capy/object"
)

type Decoder struct {
	r *bufio.Reader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{bufio.NewReader(r)}
}

func (e *Decoder) Decode() (any, error) {
	kind, err := e.r.ReadByte()
	if err != nil {
		return nil, err
	}
	err = e.r.UnreadByte()
	if err != nil {
		return nil, err
	}
	switch kind {
	case kindCommit:
		return e.DecodeCommit()
	case kindDataRoot:
		return e.DecodeDataRoot()
	case kindCollection:
		return e.DecodeCollection()
	case kindDocument:
		return e.DecodeDocument()
	case kindHash:
		return e.DecodeHash()
	case kindBytes:
		return e.DecodeBytes()
	case kindString:
		return e.DecodeString()
	case kindInt64:
		return e.DecodeInt64()
	case kindFloat64:
		return e.DecodeFloat64()
	case kindBool:
		return e.DecodeBool()
	case kindList:
		return e.DecodeList()
	case kindMap:
		return e.DecodeMap()
	default:
		return nil, fmt.Errorf("invalid codec kind %x", kind)
	}
}

func (e *Decoder) DecodeCommit() (*object.Commit, error) {
	kind, err := e.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if kind != kindCommit {
		return nil, fmt.Errorf("unexpected codec kind %x", kind)
	}
	parents, err := e.DecodeList()
	if err != nil {
		return nil, err
	}
	dataRoot, err := e.DecodeHash()
	if err != nil {
		return nil, err
	}
	commit := object.Commit{
		DataRoot: dataRoot,
		Parents:  make([]object.Hash, len(parents)),
	}
	for i, p := range parents {
		commit.Parents[i] = p.(object.Hash)
	}
	return &commit, nil
}

func (e *Decoder) DecodeDataRoot() (*object.DataRoot, error) {
	kind, err := e.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if kind != kindDataRoot {
		return nil, fmt.Errorf("unexpected codec kind %x", kind)
	}
	collections, err := e.DecodeMap()
	if err != nil {
		return nil, err
	}
	dataRoot := object.DataRoot{
		Collections: make(map[string]object.Hash, len(collections)),
	}
	for k, v := range collections {
		dataRoot.Collections[k] = v.(object.Hash)
	}
	return &dataRoot, nil
}

func (e *Decoder) DecodeCollection() (*object.Collection, error) {
	kind, err := e.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if kind != kindCollection {
		return nil, fmt.Errorf("unexpected codec kind %x", kind)
	}
	documents, err := e.DecodeMap()
	if err != nil {
		return nil, err
	}
	collection := object.Collection{
		Documents: make(map[string]object.Hash),
	}
	for k, v := range documents {
		collection.Documents[k] = v.(object.Hash)
	}
	return &collection, nil
}

func (e *Decoder) DecodeDocument() (object.Document, error) {
	kind, err := e.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if kind != kindDocument {
		return nil, fmt.Errorf("unexpected codec kind %x", kind)
	}
	return e.DecodeMap()
}

func (e *Decoder) DecodeHash() (object.Hash, error) {
	kind, err := e.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if kind != kindHash {
		return nil, fmt.Errorf("unexpected codec kind %x", kind)
	}
	size, err := e.readUint64()
	if err != nil {
		return nil, err
	}
	value := make([]byte, size)
	_, err = e.r.Read(value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (e *Decoder) DecodeBytes() ([]byte, error) {
	kind, err := e.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if kind != kindBytes {
		return nil, fmt.Errorf("unexpected codec kind %x", kind)
	}
	size, err := e.readUint64()
	if err != nil {
		return nil, err
	}
	value := make([]byte, size)
	_, err = e.r.Read(value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (e *Decoder) DecodeString() (string, error) {
	kind, err := e.r.ReadByte()
	if err != nil {
		return "", err
	}
	if kind != kindString {
		return "", fmt.Errorf("unexpected codec kind %x", kind)
	}
	size, err := e.readUint64()
	if err != nil {
		return "", err
	}
	value := make([]byte, size)
	_, err = e.r.Read(value)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (e *Decoder) DecodeInt64() (int64, error) {
	kind, err := e.r.ReadByte()
	if err != nil {
		return 0, err
	}
	if kind != kindInt64 {
		return 0, fmt.Errorf("unexpected codec kind %x", kind)
	}
	value, err := e.readUint64()
	if err != nil {
		return 0, err
	}
	return int64(value), nil
}

func (e *Decoder) DecodeFloat64() (float64, error) {
	kind, err := e.r.ReadByte()
	if err != nil {
		return 0, err
	}
	if kind != kindFloat64 {
		return 0, fmt.Errorf("unexpected codec kind %x", kind)
	}
	value, err := e.readUint64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(value), nil
}

func (e *Decoder) DecodeBool() (bool, error) {
	kind, err := e.r.ReadByte()
	if err != nil {
		return false, err
	}
	if kind != kindBool {
		return false, fmt.Errorf("unexpected codec kind %x", kind)
	}
	value, err := e.r.ReadByte()
	if err != nil {
		return false, err
	}
	return value != 0, nil
}

func (e *Decoder) DecodeList() ([]any, error) {
	kind, err := e.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if kind != kindList {
		return nil, fmt.Errorf("unexpected codec kind %x", kind)
	}
	size, err := e.readUint64()
	if err != nil {
		return nil, err
	}
	value := make([]any, size)
	for i := 0; i < int(size); i++ {
		v, err := e.Decode()
		if err != nil {
			return nil, err
		}
		value[i] = v
	}
	return value, nil
}

func (e *Decoder) DecodeMap() (map[string]any, error) {
	kind, err := e.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if kind != kindMap {
		return nil, fmt.Errorf("unexpected codec kind %x", kind)
	}
	size, err := e.readUint64()
	if err != nil {
		return nil, err
	}
	value := make(map[string]any, size)
	for i := 0; i < int(size); i++ {
		k, err := e.DecodeString()
		if err != nil {
			return nil, err
		}
		v, err := e.Decode()
		if err != nil {
			return nil, err
		}
		value[k] = v
	}
	return value, nil
}

func (e *Decoder) readUint64() (uint64, error) {
	result := uint64(0)
	for i := 0; i < 8; i++ {
		b, err := e.r.ReadByte()
		if err != nil {
			return 0, err
		}
		result |= uint64(b) << (i * 8)
	}
	return result, nil
}
