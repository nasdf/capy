package core

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

type DocumentIterator struct {
	db *DB
	it datamodel.MapIterator
}

func (i *DocumentIterator) Done() bool {
	return i.it.Done()
}

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
