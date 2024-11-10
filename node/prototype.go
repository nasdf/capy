package node

import (
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
)

func Prototype(n datamodel.Node) datamodel.NodePrototype {
	tn, ok := n.(schema.TypedNode)
	if !ok {
		return basicnode.Prototype.Any
	}
	lnk, ok := tn.Type().(*schema.TypeLink)
	if ok && lnk.HasReferencedType() {
		return bindnode.Prototype(nil, lnk.ReferencedType())
	}
	return bindnode.Prototype(nil, tn.Type())
}
