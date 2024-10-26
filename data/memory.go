package data

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal"
)

// RootTypeName is the name of the root struct type.
const RootTypeName = "__Root"

type memoryStore struct {
	linkSys linking.LinkSystem
}

func NewMemoryStore() Store {
	store := &memstore.Store{}
	linkSys := defaultLinkSystem()
	linkSys.SetReadStorage(store)
	linkSys.SetWriteStorage(store)

	return &memoryStore{
		linkSys: linkSys,
	}
}

func (m *memoryStore) Load(ctx context.Context, lnk datamodel.Link, np datamodel.NodePrototype) (datamodel.Node, error) {
	return m.linkSys.Load(linking.LinkContext{Ctx: ctx}, lnk, np)
}

func (m *memoryStore) Store(ctx context.Context, node datamodel.Node) (datamodel.Link, error) {
	return m.linkSys.Store(linking.LinkContext{Ctx: ctx}, defaultLinkPrototype, node)
}

func (m *memoryStore) LinkSystem() linking.LinkSystem {
	return m.linkSys
}

func (m *memoryStore) Traversal(ctx context.Context) traversal.Progress {
	cfg := &traversal.Config{
		Ctx:                            ctx,
		LinkSystem:                     m.linkSys,
		LinkTargetNodePrototypeChooser: defaultNodePrototypeChooser,
	}
	return traversal.Progress{
		Cfg: cfg,
	}
}
