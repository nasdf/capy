package link

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/nasdf/capy/storage"
)

// Store is a content addressable data store.
type Store struct {
	lsys linking.LinkSystem
}

// NewStore returns a new Store that uses the given storage to read and write content addressable data.
func NewStore(store storage.Storage) *Store {
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)

	return &Store{
		lsys: lsys,
	}
}

// Load returns the node matching the given link and built using the given prototype.
func (s *Store) Load(ctx context.Context, lnk datamodel.Link, np datamodel.NodePrototype) (datamodel.Node, error) {
	return s.lsys.Load(linking.LinkContext{Ctx: ctx}, lnk, np)
}

// Store writes the given node to the db and returns its link.
func (s *Store) Store(ctx context.Context, node datamodel.Node) (datamodel.Link, error) {
	return s.lsys.Store(linking.LinkContext{Ctx: ctx}, linkPrototype, node)
}

// Traversal returns a traversal.Progress configured with the default values for this db.
func (s *Store) Traversal(ctx context.Context) traversal.Progress {
	return traversal.Progress{Cfg: &traversal.Config{
		Ctx:                            ctx,
		LinkSystem:                     s.lsys,
		LinkTargetNodePrototypeChooser: prototypeChooser,
	}}
}

// GetNode returns the node at the given path starting from the given node.
func (s *Store) GetNode(ctx context.Context, path datamodel.Path, node datamodel.Node) (datamodel.Node, error) {
	return s.Traversal(ctx).Get(node, path)
}

// SetNode sets the node at the given path starting from the given node returning the updated node.
func (s *Store) SetNode(ctx context.Context, path datamodel.Path, node datamodel.Node, value datamodel.Node) (datamodel.Node, error) {
	fn := func(p traversal.Progress, n datamodel.Node) (datamodel.Node, error) {
		return value, nil
	}
	return s.Traversal(ctx).FocusedTransform(node, path, fn, true)
}
