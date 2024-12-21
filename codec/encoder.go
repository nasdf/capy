package codec

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/rodent-software/capy/object"
)

type Encoder struct {
	w *bufio.Writer
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{bufio.NewWriter(w)}
}

func (e *Encoder) Flush() error {
	return e.w.Flush()
}

func (e *Encoder) Encode(value any) error {
	switch t := value.(type) {
	case *object.Commit:
		return e.EncodeCommit(t)
	case *object.DataRoot:
		return e.EncodeDataRoot(t)
	case *object.Collection:
		return e.EncodeCollection(t)
	case object.Document:
		return e.EncodeDocument(t)
	case object.Hash:
		return e.EncodeHash(t)
	case []byte:
		return e.EncodeBytes(t)
	case string:
		return e.EncodeString(t)
	case int64:
		return e.EncodeInt64(t)
	case float64:
		return e.EncodeFloat64(t)
	case bool:
		return e.EncodeBool(t)
	case []any:
		return e.EncodeList(t)
	case map[string]any:
		return e.EncodeMap(t)
	default:
		return fmt.Errorf("no encoder for %T", value)
	}
}

func (e *Encoder) EncodeCommit(value *object.Commit) error {
	err := e.w.WriteByte(kindCommit)
	if err != nil {
		return err
	}
	parents := make([]any, len(value.Parents))
	for i, p := range value.Parents {
		parents[i] = p
	}
	err = e.EncodeList(parents)
	if err != nil {
		return err
	}
	return e.EncodeHash(value.DataRoot)
}

func (e *Encoder) EncodeDataRoot(value *object.DataRoot) error {
	err := e.w.WriteByte(kindDataRoot)
	if err != nil {
		return err
	}
	collections := make(map[string]any, len(value.Collections))
	for k, v := range value.Collections {
		collections[k] = v
	}
	return e.EncodeMap(collections)
}

func (e *Encoder) EncodeCollection(value *object.Collection) error {
	err := e.w.WriteByte(kindCollection)
	if err != nil {
		return err
	}
	documents := make(map[string]any, len(value.Documents))
	for k, v := range value.Documents {
		documents[k] = v
	}
	return e.EncodeMap(documents)
}

func (e *Encoder) EncodeDocument(value object.Document) error {
	err := e.w.WriteByte(kindDocument)
	if err != nil {
		return err
	}
	return e.EncodeMap(value)
}

func (e *Encoder) EncodeHash(value object.Hash) error {
	err := e.w.WriteByte(kindHash)
	if err != nil {
		return err
	}
	err = e.writeUint64(uint64(len(value)))
	if err != nil {
		return err
	}
	_, err = e.w.Write(value)
	return err
}

func (e *Encoder) EncodeBytes(value []byte) error {
	err := e.w.WriteByte(kindBytes)
	if err != nil {
		return err
	}
	err = e.writeUint64(uint64(len(value)))
	if err != nil {
		return err
	}
	_, err = e.w.Write(value)
	return err
}

func (e *Encoder) EncodeString(value string) error {
	err := e.w.WriteByte(kindString)
	if err != nil {
		return err
	}
	err = e.writeUint64(uint64(len(value)))
	if err != nil {
		return err
	}
	_, err = e.w.Write([]byte(value))
	return err
}

func (e *Encoder) EncodeInt64(value int64) error {
	err := e.w.WriteByte(kindInt64)
	if err != nil {
		return err
	}
	return e.writeUint64(uint64(value))
}

func (e *Encoder) EncodeFloat64(value float64) error {
	err := e.w.WriteByte(kindFloat64)
	if err != nil {
		return err
	}
	return e.writeUint64(math.Float64bits(value))
}

func (e *Encoder) EncodeBool(value bool) error {
	err := e.w.WriteByte(kindBool)
	if err != nil {
		return err
	}
	if value {
		return e.w.WriteByte(1)
	}
	return e.w.WriteByte(0)
}

func (e *Encoder) EncodeList(value []any) error {
	err := e.w.WriteByte(kindList)
	if err != nil {
		return err
	}
	err = e.writeUint64(uint64(len(value)))
	if err != nil {
		return err
	}
	for _, v := range value {
		err := e.Encode(v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) EncodeMap(value map[string]any) error {
	err := e.w.WriteByte(kindMap)
	if err != nil {
		return err
	}
	err = e.writeUint64(uint64(len(value)))
	if err != nil {
		return err
	}

	keys := make([]string, 0, len(value))
	for k := range value {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	for _, k := range keys {
		err := e.EncodeString(k)
		if err != nil {
			return err
		}
		err = e.Encode(value[k])
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) writeUint64(value uint64) error {
	for i := 0; i < 8; i++ {
		err := e.w.WriteByte(byte(value >> (i * 8)))
		if err != nil {
			return err
		}
	}
	return nil
}
