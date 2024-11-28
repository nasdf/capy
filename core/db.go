package core

import (
	"context"
	"fmt"
	"sync"

	"github.com/nasdf/capy/storage"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
)

// RootLinkKey is the name of the key for the root link.
const RootLinkKey = "root"

type DB struct {
	store    storage.Storage
	links    linking.LinkSystem
	rootLink datamodel.Link
	rootLock sync.RWMutex
}

func Open(ctx context.Context, store storage.Storage, schema string) (*DB, error) {
	links := cidlink.DefaultLinkSystem()
	links.SetReadStorage(store)
	links.SetWriteStorage(store)

	db := &DB{
		store: store,
		links: links,
	}

	rootNode, err := BuildRootNode(ctx, db, schema)
	if err != nil {
		return nil, err
	}
	rootLink, err := db.Store(ctx, rootNode)
	if err != nil {
		return nil, err
	}
	err = store.Put(ctx, RootLinkKey, []byte(rootLink.String()))
	if err != nil {
		return nil, err
	}
	db.rootLink = rootLink
	return db, nil
}

// LinkSystem returns the linking.LinkSystem used to store and load data.
func (db *DB) LinkSystem() *linking.LinkSystem {
	return &db.links
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

// RootLink returns the current root link from the db.
func (db *DB) RootLink() datamodel.Link {
	db.rootLock.RLock()
	defer db.rootLock.RUnlock()

	return db.rootLink
}

// Transaction returns a new transaction that can be used to modify documents.
func (db *DB) Transaction(ctx context.Context, readOnly bool) (*Transaction, error) {
	db.rootLock.RLock()
	defer db.rootLock.RUnlock()

	rootNode, err := db.Load(ctx, db.rootLink, basicnode.Prototype.Map)
	if err != nil {
		return nil, err
	}
	return &Transaction{
		db:       db,
		readOnly: readOnly,
		rootNode: rootNode,
		rootLink: db.rootLink,
	}, nil
}

// Commit creates a new commit from the given link using the contents of the given node.
func (db *DB) Commit(ctx context.Context, rootLink datamodel.Link, rootNode datamodel.Node) error {
	db.rootLock.Lock()
	defer db.rootLock.Unlock()

	if db.rootLink != rootLink {
		return fmt.Errorf("transaction conflict")
	}
	parentsNode, err := BuildRootParentsNode(db, rootLink)
	if err != nil {
		return err
	}
	rootPath := datamodel.ParsePath(RootParentsFieldName)
	rootNode, err = db.SetNode(ctx, rootPath, rootNode, parentsNode)
	if err != nil {
		return err
	}
	rootLink, err = db.Store(ctx, rootNode)
	if err != nil {
		return err
	}
	err = db.store.Put(ctx, RootLinkKey, []byte(rootLink.String()))
	if err != nil {
		return err
	}
	db.rootLink = rootLink
	return nil
}

// Dump returns a map of collections to document ids.
//
// This function is primarily used for testing.
func (db *DB) Dump(ctx context.Context) (map[string][]string, error) {
	rootNode, err := db.Load(ctx, db.RootLink(), basicnode.Prototype.Map)
	if err != nil {
		return nil, err
	}
	collectionsLinkNode, err := rootNode.LookupByString(RootCollectionsFieldName)
	if err != nil {
		return nil, err
	}
	collectionsLink, err := collectionsLinkNode.AsLink()
	if err != nil {
		return nil, err
	}
	collectionsNode, err := db.Load(ctx, collectionsLink, basicnode.Prototype.Map)
	if err != nil {
		return nil, err
	}
	docs := make(map[string][]string)
	iter := collectionsNode.MapIterator()
	for !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			return nil, err
		}
		collection, err := k.AsString()
		if err != nil {
			return nil, err
		}
		collectionLink, err := v.AsLink()
		if err != nil {
			return nil, err
		}
		collectionNode, err := db.Load(ctx, collectionLink, basicnode.Prototype.Map)
		if err != nil {
			return nil, err
		}
		documentsNode, err := collectionNode.LookupByString(CollectionDocumentsFieldName)
		if err != nil {
			return nil, err
		}
		documentIter := documentsNode.MapIterator()
		for !documentIter.Done() {
			k, _, err := documentIter.Next()
			if err != nil {
				return nil, err
			}
			id, err := k.AsString()
			if err != nil {
				return nil, err
			}
			docs[collection] = append(docs[collection], id)
		}
	}
	return docs, nil
}
