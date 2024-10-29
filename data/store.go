package data

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"

	// codecs need to be initialized and registered
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
)

var defaultLinkPrototype = cidlink.LinkPrototype{Prefix: cid.Prefix{
	Version:  1,    // Usually '1'.
	Codec:    0x71, // dag-cbor -- See the multicodecs table: https://github.com/multiformats/multicodec/
	MhType:   0x13, // sha2-512 -- See the multicodecs table: https://github.com/multiformats/multicodec/
	MhLength: 64,   // sha2-512 hash has a 64-byte sum.
}}

var defaultNodePrototypeChooser = traversal.LinkTargetNodePrototypeChooser(func(l datamodel.Link, lc linking.LinkContext) (datamodel.NodePrototype, error) {
	return basicnode.Prototype.Any, nil
})

// Store is a content addressable store for linked data.
type Store struct {
	linkSys linking.LinkSystem
}

// NewMemStore returns a new memory backed store.
func NewMemStore() *Store {
	store := &memstore.Store{}

	linkSys := cidlink.DefaultLinkSystem()
	linkSys.SetReadStorage(store)
	linkSys.SetWriteStorage(store)

	return &Store{
		linkSys: linkSys,
	}
}

// Load returns the node matching the given link and built using the given prototype.
func (s *Store) Load(ctx context.Context, lnk datamodel.Link, np datamodel.NodePrototype) (datamodel.Node, error) {
	return s.linkSys.Load(linking.LinkContext{Ctx: ctx}, lnk, np)
}

// Store writes the given node to the store and returns its link.
func (s *Store) Store(ctx context.Context, node datamodel.Node) (datamodel.Link, error) {
	return s.linkSys.Store(linking.LinkContext{Ctx: ctx}, defaultLinkPrototype, node)
}

// LinkSystem returns the underlying LinkSystem that is used to create and load linked data.
func (s *Store) LinkSystem() linking.LinkSystem {
	return s.linkSys
}

// Traversal returns a traversal.Progress configured with the default values for this store.
func (s *Store) Traversal(ctx context.Context) traversal.Progress {
	cfg := &traversal.Config{
		Ctx:                            ctx,
		LinkSystem:                     s.linkSys,
		LinkTargetNodePrototypeChooser: defaultNodePrototypeChooser,
	}
	return traversal.Progress{
		Cfg: cfg,
	}
}

// Export writes the contents of the node at the given link to the writer encoded as a CAR.
func (s *Store) Export(ctx context.Context, lnk datamodel.Link, out io.Writer) error {
	root, err := cid.Decode(lnk.String())
	if err != nil {
		return err
	}
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))
	// write the contents encoded as a CAR
	w, err := car.NewSelectiveWriter(ctx, &s.linkSys, root, sel.Node())
	if err != nil {
		return err
	}
	_, err = w.WriteTo(out)
	return err
}
