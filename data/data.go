package data

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"

	// codecs need to be initialized and registered
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
)

var defaultLinkSystem = func() linking.LinkSystem {
	return cidlink.DefaultLinkSystem()
}

var defaultLinkPrototype = cidlink.LinkPrototype{Prefix: cid.Prefix{
	Version:  1,    // Usually '1'.
	Codec:    0x71, // dag-cbor -- See the multicodecs table: https://github.com/multiformats/multicodec/
	MhType:   0x13, // sha2-512 -- See the multicodecs table: https://github.com/multiformats/multicodec/
	MhLength: 64,   // sha2-512 hash has a 64-byte sum.
}}

var defaultNodePrototypeChooser = traversal.LinkTargetNodePrototypeChooser(func(l datamodel.Link, lc linking.LinkContext) (datamodel.NodePrototype, error) {
	return basicnode.Prototype.Any, nil
})

type Store interface {
	Load(ctx context.Context, lnk datamodel.Link, np datamodel.NodePrototype) (datamodel.Node, error)
	Store(ctx context.Context, node datamodel.Node) (datamodel.Link, error)
	Traversal(ctx context.Context) traversal.Progress
}
