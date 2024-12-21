package codec

import (
	"bytes"
	"math"
	"testing"

	"github.com/rodent-software/capy/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testInput = []any{
	"",
	"test",
	[]byte{},
	[]byte{0, 1, 2, 3},
	int64(math.MaxInt64),
	int64(math.MinInt64),
	float64(3.14),
	true,
	false,
	[]any{},
	[]any{int64(5), "hello"},
	map[string]any{},
	map[string]any{"count": int64(9)},
	object.Sum([]byte("test")),
	&object.Commit{
		Parents:  []object.Hash{object.Sum([]byte("parent"))},
		DataRoot: object.Sum([]byte("root")),
	},
	&object.DataRoot{
		Collections: map[string]object.Hash{"User": object.Sum([]byte("User"))},
	},
	&object.Collection{
		Documents: map[string]object.Hash{"1": object.Sum([]byte("1"))},
	},
	object.Document(map[string]any{"one": int64(1), "name": "Bob"}),
}

func TestEncodeDecode(t *testing.T) {
	var buffer bytes.Buffer
	enc := NewEncoder(&buffer)
	dec := NewDecoder(&buffer)

	for _, expect := range testInput {
		buffer.Reset()

		err := enc.Encode(expect)
		require.NoError(t, err)

		err = enc.Flush()
		require.NoError(t, err)

		actual, err := dec.Decode()
		require.NoError(t, err)

		assert.Equal(t, expect, actual)
	}
}
