package core

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage"
	"github.com/ipld/go-ipld-prime/traversal"
)

// RootLinkKey is the name of the key for the root link.
const RootLinkKey = "root"

// Store is a content addressable store for linked data.
type Store struct {
	links linking.LinkSystem
	store storage.Storage
}

// Open returns a new store using the given storage implementation to persist data.
func Open(ctx context.Context, store storage.Storage) *Store {
	links := cidlink.DefaultLinkSystem()
	if r, ok := store.(storage.ReadableStorage); ok {
		links.SetReadStorage(r)
	}
	if w, ok := store.(storage.WritableStorage); ok {
		links.SetWriteStorage(w)
	}
	return &Store{
		links: links,
		store: store,
	}
}

// Load returns the node matching the given link and built using the given prototype.
func (s *Store) Load(ctx context.Context, lnk datamodel.Link, np datamodel.NodePrototype) (datamodel.Node, error) {
	return s.links.Load(linking.LinkContext{Ctx: ctx}, lnk, np)
}

// Store writes the given node to the store and returns its link.
func (s *Store) Store(ctx context.Context, node datamodel.Node) (datamodel.Link, error) {
	return s.links.Store(linking.LinkContext{Ctx: ctx}, defaultLinkPrototype, node)
}

// Traversal returns a traversal.Progress configured with the default values for this store.
func (s *Store) Traversal(ctx context.Context) traversal.Progress {
	return traversal.Progress{Cfg: defaultTraversalConfig(ctx, s.links)}
}

// RootLink returns the current root link from the store.
func (s *Store) RootLink(ctx context.Context) (datamodel.Link, error) {
	r, ok := s.store.(storage.ReadableStorage)
	if !ok {
		return nil, fmt.Errorf("storage is not readable")
	}
	data, err := r.Get(ctx, RootLinkKey)
	if err != nil {
		return nil, err
	}
	id, err := cid.Decode(string(data))
	if err != nil {
		return nil, err
	}
	return &cidlink.Link{Cid: id}, nil
}

// SetRootLink sets the store root link to the given link value.
func (s *Store) SetRootLink(ctx context.Context, lnk datamodel.Link) error {
	w, ok := s.store.(storage.WritableStorage)
	if !ok {
		return fmt.Errorf("storage is not writable")
	}
	return w.Put(ctx, RootLinkKey, []byte(lnk.String()))
}
