package core

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// Dump returns a map of collections to document ids.
//
// This function is primarily used for testing.
func Dump(ctx context.Context, store *Store, rootLink datamodel.Link) (map[string][]string, error) {
	rootNode, err := store.Load(ctx, rootLink, basicnode.Prototype.Map)
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
	collectionsNode, err := store.Load(ctx, collectionsLink, basicnode.Prototype.Map)
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
		collectionNode, err := store.Load(ctx, collectionLink, basicnode.Prototype.Map)
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
