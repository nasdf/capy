package core

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// DocumentIterator iterates over all documents in a collection.
type DocumentIterator struct {
	db *DB
	it datamodel.MapIterator
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
	doc, err := i.db.Load(ctx, lnk, basicnode.Prototype.Map)
	if err != nil {
		return "", nil, err
	}
	return id, doc, nil
}

// CommitIterator iterates over all commits in a database.
type CommitIterator struct {
	db   *DB
	next []datamodel.Link
	seen map[string]struct{}
}

// Done returns true if the iterator has no items left.
func (i *CommitIterator) Done() bool {
	return len(i.next) == 0
}

// Next returns the next commit link and commit node from the iterator.
func (i *CommitIterator) Next(ctx context.Context) (datamodel.Link, datamodel.Node, error) {
	rootLink := i.next[0]
	rootNode, err := i.db.Load(ctx, rootLink, basicnode.Prototype.Map)
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
