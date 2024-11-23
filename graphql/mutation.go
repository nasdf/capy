package graphql

import (
	"context"
	"strings"

	"github.com/nasdf/capy/node"
	"github.com/nasdf/capy/types"

	"github.com/99designs/gqlgen/graphql"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func (e *executionContext) executeMutation(ctx context.Context, set ast.SelectionSet, na datamodel.NodeAssembler) error {
	rootLink := ctx.Value(rootContextKey).(datamodel.Link)
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
			rootLink, err = e.createMutation(ctx, field, collection, va)
			if err != nil {
				return err
			}

		case strings.HasPrefix(field.Name, deleteOperationPrefix):
			collection := strings.TrimPrefix(field.Name, deleteOperationPrefix)
			rootLink, err = e.deleteMutation(ctx, field, collection, va)
			if err != nil {
				return err
			}

		default:
			return gqlerror.Errorf("unsupported mutation %s", field.Name)
		}
	}
	err = e.store.SetRootLink(ctx, rootLink)
	if err != nil {
		return err
	}
	return ma.Finish()
}

func (e *executionContext) createMutation(ctx context.Context, field graphql.CollectedField, collection string, na datamodel.NodeAssembler) (datamodel.Link, error) {
	args := field.ArgumentMap(e.params.Variables)
	builder := node.NewBuilder(e.store, e.system)

	id, err := builder.Build(ctx, collection, args["data"])
	if err != nil {
		return nil, err
	}
	rootLink := ctx.Value(rootContextKey).(datamodel.Link)
	rootNode, err := e.store.Load(ctx, rootLink, e.system.Prototype(types.RootTypeName))
	if err != nil {
		return nil, err
	}
	for k, v := range builder.Documents() {
		rootNode, err = e.store.SetNode(ctx, datamodel.ParsePath(k), rootNode, basicnode.NewLink(v))
		if err != nil {
			return nil, err
		}
	}
	rootPath := datamodel.ParsePath(types.RootParentsFieldName).AppendSegmentString("-")
	rootNode, err = e.store.SetNode(ctx, rootPath, rootNode, basicnode.NewLink(rootLink))
	if err != nil {
		return nil, err
	}
	rootLink, err = e.store.Store(ctx, rootNode)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, rootContextKey, rootLink)
	err = e.queryDocument(ctx, field, collection, id, na)
	if err != nil {
		return nil, err
	}
	return rootLink, nil
}

func (e *executionContext) deleteMutation(ctx context.Context, field graphql.CollectedField, collection string, na datamodel.NodeAssembler) (datamodel.Link, error) {
	rootLink := ctx.Value(rootContextKey).(datamodel.Link)
	rootNode, err := e.store.Load(ctx, rootLink, e.system.Prototype(types.RootTypeName))
	if err != nil {
		return nil, err
	}
	collectionNode, err := rootNode.LookupByString(collection)
	if err != nil {
		return nil, err
	}
	la, err := na.BeginList(collectionNode.Length())
	if err != nil {
		return nil, err
	}
	args := field.ArgumentMap(e.params.Variables)
	iter := collectionNode.MapIterator()
	for !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			return nil, err
		}
		val := v.(schema.TypedNode)
		key, err := k.AsString()
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, idContextKey, key)
		match, err := e.filterNode(ctx, val, args["filter"])
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		err = e.queryNode(ctx, val, field, la.AssembleValue())
		if err != nil {
			return nil, err
		}
		rootNode, err = e.store.SetNode(ctx, datamodel.ParsePath(collection+"/"+key), rootNode, nil)
		if err != nil {
			return nil, err
		}
	}
	err = la.Finish()
	if err != nil {
		return nil, err
	}
	rootPath := datamodel.ParsePath(types.RootParentsFieldName).AppendSegmentString("-")
	rootNode, err = e.store.SetNode(ctx, rootPath, rootNode, basicnode.NewLink(rootLink))
	if err != nil {
		return nil, err
	}
	rootLink, err = e.store.Store(ctx, rootNode)
	if err != nil {
		return nil, err
	}
	return rootLink, nil
}
