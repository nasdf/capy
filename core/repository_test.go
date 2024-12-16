package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepositoryInit(t *testing.T) {
	ctx := context.Background()
	schema := `type User { name: String }`
	storage := NewMemoryStorage()

	repo, err := InitRepository(ctx, storage, schema)
	require.NoError(t, err)

	assert.NotNil(t, repo.head)
	assert.NotNil(t, repo.schema)
}
