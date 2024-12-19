package schema_gen

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecute(t *testing.T) {
	_, err := Execute(`type User { name: String }`)
	require.NoError(t, err)
}
