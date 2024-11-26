package core

import (
	"context"
	"errors"
	"slices"

	"github.com/nasdf/capy/storage"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal"
)

// RootLinkKey is the name of the key for the root link.
const RootLinkKey = "root"

type Store struct {
	storage storage.Storage
	links   linking.LinkSystem
	types   *schema.TypeSystem
}

func Open(ctx context.Context, storage storage.Storage, schema string) (*Store, error) {
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

	store := &Store{
		storage: storage,
		links:   links,
		types:   types,
	}

	nb := store.Prototype(RootTypeName).NewBuilder()
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
	lnk, err := store.Store(ctx, nb.Build())
	if err != nil {
		return nil, err
	}
	err = store.SetRootLink(ctx, lnk)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func Load(ctx context.Context, storage storage.Storage) (string, *Store, error) {
	links := cidlink.DefaultLinkSystem()
	links.SetReadStorage(storage)
	links.SetWriteStorage(storage)

	store := &Store{
		storage: storage,
		links:   links,
	}

	rootLink, err := store.RootLink(ctx)
	if err != nil {
		return "", nil, err
	}
	rootNode, err := store.Load(ctx, rootLink, basicnode.Prototype.Any)
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
	store.types = types
	return schema, store, nil
}

// RootNode returns the root node from the store.
func (s *Store) RootNode(ctx context.Context) (datamodel.Node, error) {
	rootLink, err := s.RootLink(ctx)
	if err != nil {
		return nil, err
	}
	return s.Load(ctx, rootLink, s.Prototype(RootTypeName))
}

// RootLink returns the current root link from the store.
func (s *Store) RootLink(ctx context.Context) (datamodel.Link, error) {
	data, err := s.storage.Get(ctx, RootLinkKey)
	if err != nil {
		return nil, err
	}
	id, err := cid.Decode(string(data))
	if err != nil {
		return nil, err
	}
	return cidlink.Link{Cid: id}, nil
}

// SetRootLink sets the store root link to the given link value.
func (s *Store) SetRootLink(ctx context.Context, lnk datamodel.Link) error {
	return s.storage.Put(ctx, RootLinkKey, []byte(lnk.String()))
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

// LinkSystem returns the linking.LinkSystem used to store and load data.
func (s *Store) LinkSystem() *linking.LinkSystem {
	return &s.links
}

// TypeSystem returns the schema.TypeSystem containing all defined types.
func (s *Store) TypeSystem() *schema.TypeSystem {
	return s.types
}

// IsRelation returns true if the given type is a relation.
func (s *Store) IsRelation(t schema.Type) bool {
	return t.TypeKind() == schema.TypeKind_String && slices.Contains(s.types.Names(), t.Name()+DocumentSuffix)
}

// Type returns the type with a matching name.
func (s *Store) Type(name string) schema.Type {
	return s.types.TypeByName(name)
}

// Prototype returns the NodePrototype for the type with a matching name.
func (s *Store) Prototype(name string) datamodel.NodePrototype {
	return bindnode.Prototype(nil, s.Type(name))
}
