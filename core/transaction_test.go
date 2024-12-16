package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionCreateDocument(t *testing.T) {
	ctx := context.Background()
	schema := `type User { name: String }`
	storage := NewMemoryStorage()

	repo, err := InitRepository(ctx, storage, schema)
	require.NoError(t, err)

	tx, err := repo.Transaction(ctx, repo.head)
	require.NoError(t, err)

	expect := map[string]any{"name": "Bob"}
	id, err := tx.CreateDocument(ctx, "User", expect)
	require.NoError(t, err)

	actual, err := tx.ReadDocument(ctx, "User", id)
	require.NoError(t, err)

	assert.Equal(t, expect, actual)
}
