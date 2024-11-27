package core

import (
	"context"
	"errors"

	"github.com/nasdf/capy/storage"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

// RootLinkKey is the name of the key for the root link.
const RootLinkKey = "root"

type DB struct {
	storage storage.Storage
	links   linking.LinkSystem
	types   *schema.TypeSystem
}

func Open(ctx context.Context, storage storage.Storage, schema string) (*DB, error) {
	as, err := gqlparser.LoadSchema(&ast.Source{Input: schema})
	if err != nil {
		return nil, err
	}
	types, errs := SpawnTypeSystem(as)
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	links := cidlink.DefaultLinkSystem()
	links.SetReadStorage(storage)
	links.SetWriteStorage(storage)

	db := &DB{
		storage: storage,
		links:   links,
		types:   types,
	}

	nb := db.Prototype(RootTypeName).NewBuilder()
	mb, err := nb.BeginMap(1)
	if err != nil {
		return nil, err
	}
	na, err := mb.AssembleEntry(RootSchemaFieldName)
	if err != nil {
		return nil, err
	}
	err = na.AssignString(schema)
	if err != nil {
		return nil, err
	}
	lnk, err := db.Store(ctx, nb.Build())
	if err != nil {
		return nil, err
	}
	err = db.SetRootLink(ctx, lnk)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func Load(ctx context.Context, storage storage.Storage) (string, *DB, error) {
	links := cidlink.DefaultLinkSystem()
	links.SetReadStorage(storage)
	links.SetWriteStorage(storage)

	db := &DB{
		storage: storage,
		links:   links,
	}

	rootLink, err := db.RootLink(ctx)
	if err != nil {
		return "", nil, err
	}
	rootNode, err := db.Load(ctx, rootLink, basicnode.Prototype.Any)
	if err != nil {
		return "", nil, err
	}
	schemaNode, err := rootNode.LookupByString(RootSchemaFieldName)
	if err != nil {
		return "", nil, err
	}
	schema, err := schemaNode.AsString()
	if err != nil {
		return "", nil, err
	}
	as, err := gqlparser.LoadSchema(&ast.Source{Input: schema})
	if err != nil {
		return "", nil, err
	}
	types, errs := SpawnTypeSystem(as)
	if len(errs) > 0 {
		return "", nil, errors.Join(errs...)
	}
	db.types = types
	return schema, db, nil
}

// LinkSystem returns the linking.LinkSystem used to store and load data.
func (db *DB) LinkSystem() *linking.LinkSystem {
	return &db.links
}

// TypeSystem returns the schema.TypeSystem containing all defined types.
func (db *DB) TypeSystem() *schema.TypeSystem {
	return db.types
}

// Type returns the type with a matching name.
func (db *DB) Type(name string) schema.Type {
	return db.types.TypeByName(name)
}

// Prototype returns the NodePrototype for the type with a matching name.
func (db *DB) Prototype(name string) datamodel.NodePrototype {
	return bindnode.Prototype(nil, db.Type(name))
}

// RootNode returns the root node from the db.
func (db *DB) RootNode(ctx context.Context) (datamodel.Node, error) {
	rootLink, err := db.RootLink(ctx)
	if err != nil {
		return nil, err
	}
	return db.Load(ctx, rootLink, db.Prototype(RootTypeName))
}

// RootLink returns the current root link from the db.
func (db *DB) RootLink(ctx context.Context) (datamodel.Link, error) {
	data, err := db.storage.Get(ctx, RootLinkKey)
	if err != nil {
		return nil, err
	}
	id, err := cid.Decode(string(data))
	if err != nil {
		return nil, err
	}
	return cidlink.Link{Cid: id}, nil
}

// SetRootLink sets the db root link to the given link value.
func (db *DB) SetRootLink(ctx context.Context, lnk datamodel.Link) error {
	return db.storage.Put(ctx, RootLinkKey, []byte(lnk.String()))
}

// Load returns the node matching the given link and built using the given prototype.
func (db *DB) Load(ctx context.Context, lnk datamodel.Link, np datamodel.NodePrototype) (datamodel.Node, error) {
	return db.links.Load(linking.LinkContext{Ctx: ctx}, lnk, np)
}

// Store writes the given node to the db and returns its link.
func (db *DB) Store(ctx context.Context, node datamodel.Node) (datamodel.Link, error) {
	return db.links.Store(linking.LinkContext{Ctx: ctx}, defaultLinkPrototype, node)
}

// Traversal returns a traversal.Progress configured with the default values for this db.
func (db *DB) Traversal(ctx context.Context) traversal.Progress {
	return traversal.Progress{Cfg: defaultTraversalConfig(ctx, db.links)}
}

// GetNode returns the node at the given path starting from the given node.
func (db *DB) GetNode(ctx context.Context, path datamodel.Path, node datamodel.Node) (datamodel.Node, error) {
	return db.Traversal(ctx).Get(node, path)
}

// SetNode sets the node at the given path starting from the given node returning the updated node.
func (db *DB) SetNode(ctx context.Context, path datamodel.Path, node datamodel.Node, value datamodel.Node) (datamodel.Node, error) {
	fn := func(p traversal.Progress, n datamodel.Node) (datamodel.Node, error) {
		return value, nil
	}
	return db.Traversal(ctx).FocusedTransform(node, path, fn, true)
}

// Transaction returns a new transaction that can be used to modify documents.
func (db *DB) Transaction(ctx context.Context, readOnly bool) (*Transaction, error) {
	rootLink, err := db.RootLink(ctx)
	if err != nil {
		return nil, err
	}
	rootNode, err := db.Load(ctx, rootLink, db.Prototype(RootTypeName))
	if err != nil {
		return nil, err
	}
	return &Transaction{
		DB:       db,
		readOnly: readOnly,
		rootNode: rootNode,
		rootLink: rootLink,
	}, nil
}
