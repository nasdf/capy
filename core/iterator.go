package core

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// DocumentIterator iterates over all documents in a collection.
type DocumentIterator struct {
	store *Store
	it    datamodel.MapIterator
}

// DocumentIterator returns a new iterator that can be used to iterate through all documents in a collection.
func (t *Transaction) DocumentIterator(ctx context.Context, collection string) (*DocumentIterator, error) {
	documentsPath := DocumentsPath(collection)
	documentsNode, err := t.store.GetNode(ctx, documentsPath, t.rootNode)
	if err != nil {
		return nil, err
	}
	return &DocumentIterator{
		store: t.store,
		it:    documentsNode.MapIterator(),
	}, nil
}

// Done returns true if the iterator has no items left.
func (i *DocumentIterator) Done() bool {
	return i.it.Done()
}

// Next returns the next document id and document node from the iterator.
func (i *DocumentIterator) Next(ctx context.Context) (string, datamodel.Node, error) {
	k, v, err := i.it.Next()
	if err != nil {
		return "", nil, err
	}
	id, err := k.AsString()
	if err != nil {
		return "", nil, err
	}
	lnk, err := v.AsLink()
	if err != nil {
		return "", nil, err
	}
	doc, err := i.store.Load(ctx, lnk, basicnode.Prototype.Map)
	if err != nil {
		return "", nil, err
	}
	return id, doc, nil
}

// ParentIterator iterates over all parents of a root node.
type ParentIterator struct {
	store *Store
	next  []datamodel.Link
	seen  map[string]struct{}
}

// ParentIterator returns a new iterator that can be used to iterate through all parents of a root node.
func (s *Store) ParentIterator(rootLink datamodel.Link) *ParentIterator {
	return &ParentIterator{
		store: s,
		next:  []datamodel.Link{rootLink},
		seen:  make(map[string]struct{}),
	}
}

// Done returns true if the iterator has no items left.
func (i *ParentIterator) Done() bool {
	return len(i.next) == 0
}

// Next returns the next parent link and parent node from the iterator.
func (i *ParentIterator) Next(ctx context.Context) (datamodel.Link, datamodel.Node, error) {
	rootLink := i.next[0]
	rootNode, err := i.store.Load(ctx, rootLink, basicnode.Prototype.Map)
	if err != nil {
		return nil, nil, err
	}
	parentsNode, err := rootNode.LookupByString(RootParentsFieldName)
	if err != nil {
		return nil, nil, err
	}
	iter := parentsNode.ListIterator()
	for iter != nil && !iter.Done() {
		_, v, err := iter.Next()
		if err != nil {
			return nil, nil, err
		}
		lnk, err := v.AsLink()
		if err != nil {
			return nil, nil, err
		}
		_, ok := i.seen[lnk.String()]
		if ok {
			continue
		}
		i.seen[lnk.String()] = struct{}{}
		i.next = append(i.next, lnk)
	}
	i.next = i.next[1:]
	return rootLink, rootNode, nil
}
