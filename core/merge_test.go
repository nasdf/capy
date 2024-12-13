package core

import (
	"context"
	"testing"

	"github.com/nasdf/capy/link"
	"github.com/nasdf/capy/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeSimple(t *testing.T) {
	ctx := context.Background()
	links := link.NewStore(storage.NewMemory())
	inputSchema := "type User { name: String }"

	rootNode, err := BuildInitialRootNode(ctx, links, inputSchema)
	require.NoError(t, err)

	rootLink, err := links.Store(ctx, rootNode)
	require.NoError(t, err)

	store, err := NewStore(ctx, links, rootLink)
	require.NoError(t, err)

	txA, err := store.Transaction(ctx)
	require.NoError(t, err)

	txB, err := store.Transaction(ctx)
	require.NoError(t, err)

	_, err = txA.CreateDocument(ctx, "User", map[string]any{"name": "Bob"})
	require.NoError(t, err)

	_, err = txB.CreateDocument(ctx, "User", map[string]any{"name": "Alice"})
	require.NoError(t, err)

	linkA, err := txA.Commit(ctx)
	require.NoError(t, err)

	linkB, err := txB.Commit(ctx)
	require.NoError(t, err)

	err = store.Merge(ctx, linkA)
	require.NoError(t, err)

	err = store.Merge(ctx, linkB)
	require.NoError(t, err)
}

func TestMergeConflict(t *testing.T) {
	ctx := context.Background()
	links := link.NewStore(storage.NewMemory())
	inputSchema := "type User { name: String }"

	rootNode, err := BuildInitialRootNode(ctx, links, inputSchema)
	require.NoError(t, err)

	rootLink, err := links.Store(ctx, rootNode)
	require.NoError(t, err)

	store, err := NewStore(ctx, links, rootLink)
	require.NoError(t, err)

	txA, err := store.Transaction(ctx)
	require.NoError(t, err)

	id, err := txA.CreateDocument(ctx, "User", map[string]any{"name": "Alice"})
	require.NoError(t, err)

	linkA, err := txA.Commit(ctx)
	require.NoError(t, err)

	err = store.Merge(ctx, linkA)
	require.NoError(t, err)

	txB, err := store.Transaction(ctx)
	require.NoError(t, err)

	err = txB.PatchDocument(ctx, "User", id, map[string]any{"name": map[string]any{"set": "Bob"}})
	require.NoError(t, err)

	linkB, err := txB.Commit(ctx)
	require.NoError(t, err)

	txC, err := store.Transaction(ctx)
	require.NoError(t, err)

	err = txC.PatchDocument(ctx, "User", id, map[string]any{"name": map[string]any{"set": "Chad"}})
	require.NoError(t, err)

	linkC, err := txC.Commit(ctx)
	require.NoError(t, err)

	err = store.Merge(ctx, linkB)
	require.NoError(t, err)

	err = store.Merge(ctx, linkC)
	require.NoError(t, err)

	txD, err := store.Transaction(ctx)
	require.NoError(t, err)

	docNode, err := txD.ReadDocument(ctx, "User", id)
	require.NoError(t, err)

	nameNode, err := docNode.LookupByString("name")
	require.NoError(t, err)

	name, err := nameNode.AsString()
	require.NoError(t, err)

	assert.Equal(t, "Chad", name)
}
