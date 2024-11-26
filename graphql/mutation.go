package graphql

import (
	"context"
	"strings"

	"github.com/nasdf/capy/core"

	"github.com/99designs/gqlgen/graphql"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func (e *executionContext) executeMutation(ctx context.Context, set ast.SelectionSet, na datamodel.NodeAssembler) error {
	fields := e.collectFields(set, "Mutation")
	ma, err := na.BeginMap(int64(len(fields)))
	if err != nil {
		return err
	}
	for _, field := range fields {
		va, err := ma.AssembleEntry(field.Alias)
		if err != nil {
			return err
		}
		switch {
		case strings.HasPrefix(field.Name, createOperationPrefix):
			collection := strings.TrimPrefix(field.Name, createOperationPrefix)
			err = e.createMutation(ctx, field, collection, va)
			if err != nil {
				return err
			}

		case strings.HasPrefix(field.Name, updateOperationPrefix):
			collection := strings.TrimPrefix(field.Name, updateOperationPrefix)
			err = e.updateMutation(ctx, field, collection, va)
			if err != nil {
				return err
			}

		case strings.HasPrefix(field.Name, deleteOperationPrefix):
			collection := strings.TrimPrefix(field.Name, deleteOperationPrefix)
			err = e.deleteMutation(ctx, field, collection, va)
			if err != nil {
				return err
			}

		default:
			return gqlerror.Errorf("unsupported mutation %s", field.Name)
		}
	}
	err = e.store.SetRootLink(ctx, e.rootLink)
	if err != nil {
		return err
	}
	return ma.Finish()
}

func (e *executionContext) createMutation(ctx context.Context, field graphql.CollectedField, collection string, na datamodel.NodeAssembler) error {
	args := field.ArgumentMap(e.params.Variables)

	id, err := e.createDocument(ctx, collection, args["data"])
	if err != nil {
		return err
	}
	return e.queryDocument(ctx, field, collection, id, na)
}

func (e *executionContext) updateMutation(ctx context.Context, field graphql.CollectedField, collection string, na datamodel.NodeAssembler) error {
	rootNode, err := e.store.Load(ctx, e.rootLink, e.store.Prototype(core.RootTypeName))
	if err != nil {
		return err
	}
	collectionNode, err := rootNode.LookupByString(collection)
	if err != nil {
		return err
	}
	var ids []string
	args := field.ArgumentMap(e.params.Variables)
	iter := collectionNode.MapIterator()
	for !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			return err
		}
		val := v.(schema.TypedNode)
		key, err := k.AsString()
		if err != nil {
			return err
		}
		ctx = context.WithValue(ctx, idContextKey, key)
		match, err := e.filterDocument(ctx, val, args["filter"])
		if err != nil {
			return err
		}
		if !match {
			continue
		}
		err = e.patchDocument(ctx, collection, key, val, args["patch"])
		if err != nil {
			return err
		}
		ids = append(ids, key)
	}
	la, err := na.BeginList(collectionNode.Length())
	if err != nil {
		return err
	}
	for _, id := range ids {
		err = e.queryDocument(ctx, field, collection, id, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func (e *executionContext) deleteMutation(ctx context.Context, field graphql.CollectedField, collection string, na datamodel.NodeAssembler) error {
	rootNode, err := e.store.Load(ctx, e.rootLink, e.store.Prototype(core.RootTypeName))
	if err != nil {
		return err
	}
	collectionNode, err := rootNode.LookupByString(collection)
	if err != nil {
		return err
	}
	la, err := na.BeginList(collectionNode.Length())
	if err != nil {
		return err
	}
	args := field.ArgumentMap(e.params.Variables)
	iter := collectionNode.MapIterator()
	for !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			return err
		}
		val := v.(schema.TypedNode)
		key, err := k.AsString()
		if err != nil {
			return err
		}
		ctx = context.WithValue(ctx, idContextKey, key)
		match, err := e.filterDocument(ctx, val, args["filter"])
		if err != nil {
			return err
		}
		if !match {
			continue
		}
		err = e.queryNode(ctx, val, field, la.AssembleValue())
		if err != nil {
			return err
		}
		rootNode, err = e.store.SetNode(ctx, datamodel.ParsePath(collection+"/"+key), rootNode, nil)
		if err != nil {
			return err
		}
	}
	rootPath := datamodel.ParsePath(core.RootParentsFieldName).AppendSegmentString("-")
	rootNode, err = e.store.SetNode(ctx, rootPath, rootNode, basicnode.NewLink(e.rootLink))
	if err != nil {
		return err
	}
	e.rootLink, err = e.store.Store(ctx, rootNode)
	if err != nil {
		return err
	}
	return la.Finish()
}
