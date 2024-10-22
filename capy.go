package capy

import (
	"context"

	"github.com/nasdf/capy/query"
	"github.com/nasdf/capy/schema"

	"github.com/ipfs/go-cid"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	ipldschema "github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

var LinkPrototype = cidlink.LinkPrototype{Prefix: cid.Prefix{
	Version:  1,    // Usually '1'.
	Codec:    0x71, // dag-cbor -- See the multicodecs table: https://github.com/multiformats/multicodec/
	MhType:   0x13, // sha2-512 -- See the multicodecs table: https://github.com/multiformats/multicodec/
	MhLength: 64,   // sha2-512 hash has a 64-byte sum.
}}

var LinkPrototypeChooser = traversal.LinkTargetNodePrototypeChooser(func(l datamodel.Link, lc linking.LinkContext) (datamodel.NodePrototype, error) {
	return basicnode.Prototype.Any, nil
})

type DB struct {
	// linkSys is the linking system used to store and load linked data.
	linkSys linking.LinkSystem
	// typeSys is the TypeSystem containing all user defined types.
	typeSys *ipldschema.TypeSystem
	// schema contains the generated GraphQL schema.
	schema *ast.Schema
	// rootLnk is a link to the root data node.
	rootLnk datamodel.Link
}

func New(ctx context.Context, schemaSrc string) (*DB, error) {
	// parse the user provided schema
	inputSchema, err := gqlparser.LoadSchema(&ast.Source{
		Input: schemaSrc,
	})
	if err != nil {
		return nil, err
	}
	// generate a TypeSystem from the user defined types
	typeSys, err := schema.SpawnTypeSystem(inputSchema)
	if err != nil {
		return nil, err
	}
	// generate a GraphQL schema containing all operations and types
	genSchema, err := schema.Generate(typeSys)
	if err != nil {
		return nil, err
	}

	store := &memstore.Store{}
	linkSys := cidlink.DefaultLinkSystem()
	linkSys.SetReadStorage(store)
	linkSys.SetWriteStorage(store)

	// create an empty root node
	rootType := typeSys.TypeByName(schema.RootTypeName)
	rootNode := bindnode.Prototype(nil, rootType).NewBuilder().Build()

	rootLnk, err := linkSys.Store(linking.LinkContext{Ctx: ctx}, LinkPrototype, rootNode)
	if err != nil {
		return nil, err
	}

	return &DB{
		linkSys: linkSys,
		typeSys: typeSys,
		schema:  genSchema,
		rootLnk: rootLnk,
	}, nil
}

func (db *DB) TypeSystem() *ipldschema.TypeSystem {
	return db.typeSys
}

func (db *DB) GetRootLink() datamodel.Link {
	return db.rootLnk
}

func (db *DB) SetRootLink(lnk datamodel.Link) {
	db.rootLnk = lnk
}

func (db *DB) Load(ctx context.Context, lnk datamodel.Link) (datamodel.Node, error) {
	return db.linkSys.Load(linking.LinkContext{Ctx: ctx}, lnk, basicnode.Prototype.Any)
}

func (db *DB) Store(ctx context.Context, node datamodel.Node) (datamodel.Link, error) {
	return db.linkSys.Store(linking.LinkContext{Ctx: ctx}, LinkPrototype, node)
}

func (db *DB) Traversal(ctx context.Context) traversal.Progress {
	cfg := &traversal.Config{
		Ctx:                            ctx,
		LinkSystem:                     db.linkSys,
		LinkTargetNodePrototypeChooser: LinkPrototypeChooser,
	}
	return traversal.Progress{
		Cfg: cfg,
	}
}

func (db *DB) Execute(ctx context.Context, params *query.Params) (any, error) {
	planNode, err := query.Parse(db.schema, params)
	if err != nil {
		return nil, err
	}
	return planNode.Execute(ctx, db)
}
