package core

import (
	"context"
	"testing"

	"github.com/nasdf/capy/link"
	"github.com/nasdf/capy/storage"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeBaseSimple(t *testing.T) {
	ctx := context.Background()
	links := link.NewStore(storage.NewMemory())
	inputSchema := "type User { name: String }"

	rootNode, err := BuildInitialRootNode(ctx, links, inputSchema)
	require.NoError(t, err)

	rootLink, err := links.Store(ctx, rootNode)
	require.NoError(t, err)

	store, err := NewStore(ctx, links, rootLink)
	require.NoError(t, err)

	txA, err := store.Branch(ctx, store.RootLink())
	require.NoError(t, err)

	txB, err := store.Branch(ctx, store.RootLink())
	require.NoError(t, err)

	_, err = txA.CreateDocument(ctx, "User", map[string]any{"name": "Bob"})
	require.NoError(t, err)

	_, err = txB.CreateDocument(ctx, "User", map[string]any{"name": "Alice"})
	require.NoError(t, err)

	linkA, err := txA.Commit(ctx)
	require.NoError(t, err)

	linkB, err := txB.Commit(ctx)
	require.NoError(t, err)

	bases, err := store.MergeBase(ctx, linkA, linkB)
	require.NoError(t, err)

	require.Len(t, bases, 1)
	assert.Equal(t, rootLink, bases[0])
}

func TestMergeBaseFastForward(t *testing.T) {
	ctx := context.Background()
	links := link.NewStore(storage.NewMemory())
	inputSchema := "type User { name: String }"

	rootNode, err := BuildInitialRootNode(ctx, links, inputSchema)
	require.NoError(t, err)

	rootLink, err := links.Store(ctx, rootNode)
	require.NoError(t, err)

	store, err := NewStore(ctx, links, rootLink)
	require.NoError(t, err)

	txA, err := store.Branch(ctx, store.RootLink())
	require.NoError(t, err)

	linkA, err := txA.Commit(ctx)
	require.NoError(t, err)

	txB, err := store.Branch(ctx, store.RootLink())
	require.NoError(t, err)

	linkB, err := txB.Commit(ctx)
	require.NoError(t, err)

	bases, err := store.MergeBase(ctx, linkA, linkB)
	require.NoError(t, err)

	require.Len(t, bases, 1)
	assert.Equal(t, linkA, bases[0])
}

func TestIndependentsSimple(t *testing.T) {
	ctx := context.Background()
	links := link.NewStore(storage.NewMemory())
	inputSchema := "type User { name: String }"

	rootNode, err := BuildInitialRootNode(ctx, links, inputSchema)
	require.NoError(t, err)

	rootLink, err := links.Store(ctx, rootNode)
	require.NoError(t, err)

	store, err := NewStore(ctx, links, rootLink)
	require.NoError(t, err)

	commits := make([]datamodel.Link, 5)
	for i := 0; i < len(commits); i++ {
		tx, err := store.Branch(ctx, store.RootLink())
		require.NoError(t, err)

		rootLink, err = tx.Commit(ctx)
		require.NoError(t, err)

		err = store.Merge(ctx, rootLink)
		require.NoError(t, err)

		commits[i] = rootLink
	}

	results, err := store.Independents(ctx, commits)
	require.NoError(t, err)

	require.Len(t, results, 1)
	assert.Equal(t, results[0], rootLink)
}
