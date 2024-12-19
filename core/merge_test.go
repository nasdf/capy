package core

import (
	"context"
	"testing"

	"github.com/nasdf/capy/object"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeBaseSimple(t *testing.T) {
	ctx := context.Background()
	schema := `type User { name: String }`
	storage := NewMemoryStorage()

	repo, err := InitRepository(ctx, storage, schema)
	require.NoError(t, err)

	head := repo.Head()

	txA, err := repo.Transaction(ctx, head)
	require.NoError(t, err)

	txB, err := repo.Transaction(ctx, head)
	require.NoError(t, err)

	_, err = txA.CreateDocument(ctx, "User", map[string]any{"name": "Bob"})
	require.NoError(t, err)

	_, err = txB.CreateDocument(ctx, "User", map[string]any{"name": "Alice"})
	require.NoError(t, err)

	hashA, err := txA.Commit(ctx)
	require.NoError(t, err)

	hashB, err := txB.Commit(ctx)
	require.NoError(t, err)

	bases, err := repo.mergeBase(ctx, hashA, hashB)
	require.NoError(t, err)

	require.Len(t, bases, 1)
	assert.Equal(t, head, bases[0])
}

func TestMergeBaseFastForward(t *testing.T) {
	ctx := context.Background()
	schema := `type User { name: String }`
	storage := NewMemoryStorage()

	repo, err := InitRepository(ctx, storage, schema)
	require.NoError(t, err)

	txA, err := repo.Transaction(ctx, repo.Head())
	require.NoError(t, err)

	hashA, err := txA.Commit(ctx)
	require.NoError(t, err)

	txB, err := repo.Transaction(ctx, hashA)
	require.NoError(t, err)

	hashB, err := txB.Commit(ctx)
	require.NoError(t, err)

	bases, err := repo.mergeBase(ctx, hashA, hashB)
	require.NoError(t, err)

	require.Len(t, bases, 1)
	assert.Equal(t, hashA, bases[0])
}

func TestMergeSimple(t *testing.T) {
	ctx := context.Background()
	schema := `type User { name: String }`
	storage := NewMemoryStorage()

	repo, err := InitRepository(ctx, storage, schema)
	require.NoError(t, err)

	txA, err := repo.Transaction(ctx, repo.Head())
	require.NoError(t, err)

	txB, err := repo.Transaction(ctx, repo.Head())
	require.NoError(t, err)

	createA := map[string]any{"name": "Alice"}
	idA, err := txA.CreateDocument(ctx, "User", createA)
	require.NoError(t, err)

	createB := map[string]any{"name": "Bob"}
	idB, err := txB.CreateDocument(ctx, "User", createB)
	require.NoError(t, err)

	hashA, err := txA.Commit(ctx)
	require.NoError(t, err)

	hashB, err := txB.Commit(ctx)
	require.NoError(t, err)

	err = repo.Merge(ctx, hashA)
	require.NoError(t, err)

	err = repo.Merge(ctx, hashB)
	require.NoError(t, err)

	txC, err := repo.Transaction(ctx, repo.Head())
	require.NoError(t, err)

	docA, err := txC.ReadDocument(ctx, "User", idA)
	require.NoError(t, err)

	docB, err := txC.ReadDocument(ctx, "User", idB)
	require.NoError(t, err)

	assert.Equal(t, createA, docA)
	assert.Equal(t, createB, docB)
}

func TestMergeConflict(t *testing.T) {
	ctx := context.Background()
	schema := `type User { name: String }`
	storage := NewMemoryStorage()

	repo, err := InitRepository(ctx, storage, schema)
	require.NoError(t, err)

	txA, err := repo.Transaction(ctx, repo.Head())
	require.NoError(t, err)

	id, err := txA.CreateDocument(ctx, "User", map[string]any{"name": "Alice"})
	require.NoError(t, err)

	hashA, err := txA.Commit(ctx)
	require.NoError(t, err)

	err = repo.Merge(ctx, hashA)
	require.NoError(t, err)

	txB, err := repo.Transaction(ctx, repo.Head())
	require.NoError(t, err)

	err = txB.PatchDocument(ctx, "User", id, map[string]any{"name": map[string]any{"set": "Bob"}})
	require.NoError(t, err)

	hashB, err := txB.Commit(ctx)
	require.NoError(t, err)

	txC, err := repo.Transaction(ctx, repo.Head())
	require.NoError(t, err)

	err = txC.PatchDocument(ctx, "User", id, map[string]any{"name": map[string]any{"set": "Chad"}})
	require.NoError(t, err)

	hashC, err := txC.Commit(ctx)
	require.NoError(t, err)

	err = repo.Merge(ctx, hashB)
	require.NoError(t, err)

	err = repo.Merge(ctx, hashC)
	require.NoError(t, err)

	txD, err := repo.Transaction(ctx, repo.Head())
	require.NoError(t, err)

	doc, err := txD.ReadDocument(ctx, "User", id)
	require.NoError(t, err)

	assert.Equal(t, "Chad", doc["name"])
}

func TestIndependentsSimple(t *testing.T) {
	ctx := context.Background()
	schema := `type User { name: String }`
	storage := NewMemoryStorage()

	repo, err := InitRepository(ctx, storage, schema)
	require.NoError(t, err)

	var head object.Hash
	commits := make([]object.Hash, 5)
	for i := 0; i < len(commits); i++ {
		tx, err := repo.Transaction(ctx, repo.Head())
		require.NoError(t, err)

		head, err = tx.Commit(ctx)
		require.NoError(t, err)

		err = repo.Merge(ctx, head)
		require.NoError(t, err)

		commits[i] = head
	}

	results, err := repo.independents(ctx, commits)
	require.NoError(t, err)

	require.Len(t, results, 1)
	assert.Equal(t, results[0], head)
}
