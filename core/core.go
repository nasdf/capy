package core

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal"

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
	tn, ok := lc.LinkNode.(schema.TypedNode)
	if !ok {
		return basicnode.Prototype.Any, nil
	}
	lnk, ok := tn.Type().(*schema.TypeLink)
	if ok && lnk.HasReferencedType() {
		return bindnode.Prototype(nil, lnk.ReferencedType()), nil
	}
	return bindnode.Prototype(nil, tn.Type()), nil
})

func defaultTraversalConfig(ctx context.Context, linkSys linking.LinkSystem) *traversal.Config {
	return &traversal.Config{
		Ctx:                            ctx,
		LinkSystem:                     linkSys,
		LinkTargetNodePrototypeChooser: defaultNodePrototypeChooser,
	}
}
