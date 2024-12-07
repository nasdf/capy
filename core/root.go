package core

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/node/basicnode"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

// BuildRootNode returns a new root node with the collections defined in the given schema.
func BuildRootNode(ctx context.Context, store *Store, schema string) (datamodel.Node, error) {
	s, err := gqlparser.LoadSchema(&ast.Source{Input: schema})
	if err != nil {
		return nil, err
	}
	schemaLink, err := store.Store(ctx, basicnode.NewString(schema))
	if err != nil {
		return nil, err
	}
	collectionsNode, err := BuildRootCollectionsNode(ctx, store, s)
	if err != nil {
		return nil, err
	}
	collectionsLink, err := store.Store(ctx, collectionsNode)
	if err != nil {
		return nil, err
	}
	parentsNode, err := BuildRootParentsNode()
	if err != nil {
		return nil, err
	}
	return qp.BuildMap(basicnode.Prototype.Map, 3, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, RootSchemaFieldName, qp.Link(schemaLink))
		qp.MapEntry(ma, RootCollectionsFieldName, qp.Link(collectionsLink))
		qp.MapEntry(ma, RootParentsFieldName, qp.Node(parentsNode))
	})
}

// BuildRoootParentsNode returns a new parents field node node containing the given parent links.
func BuildRootParentsNode(parents ...datamodel.Link) (datamodel.Node, error) {
	return qp.BuildList(basicnode.Prototype.List, int64(len(parents)), func(la datamodel.ListAssembler) {
		for _, l := range parents {
			qp.ListEntry(la, qp.Link(l))
		}
	})
}

// BuildRootCollectionsNode returns a new collections field node containing the collections defined in the given schema.
func BuildRootCollectionsNode(ctx context.Context, store *Store, s *ast.Schema) (datamodel.Node, error) {
	fields := make(map[string]datamodel.Link)
	for _, def := range s.Types {
		if def.BuiltIn || def.Kind != ast.Object {
			continue
		}
		node, err := BuildCollectionNode()
		if err != nil {
			return nil, err
		}
		lnk, err := store.Store(ctx, node)
		if err != nil {
			return nil, err
		}
		fields[def.Name] = lnk
	}
	return qp.BuildMap(basicnode.Prototype.Map, int64(len(fields)), func(ma datamodel.MapAssembler) {
		for k, v := range fields {
			qp.MapEntry(ma, k, qp.Link(v))
		}
	})
}

// BuildCollectionNode returns a new collection node with default field values.
func BuildCollectionNode() (datamodel.Node, error) {
	return qp.BuildMap(basicnode.Prototype.Map, 1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, CollectionDocumentsFieldName, qp.Map(0, func(ma datamodel.MapAssembler) {}))
	})
}
