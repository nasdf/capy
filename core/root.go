package core

import (
	"context"

	"github.com/nasdf/capy/link"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	// RootParentsFieldName is the name of the parents field on a root.
	RootParentsFieldName = "Parents"
	// RootSchemaFieldName is the name of the schema field on a root.
	RootSchemaFieldName = "Schema"
	// RootCollectionsFieldName is the name of the collections field on a root.
	RootCollectionsFieldName = "Collections"
	// CollectionDocumentsFieldName is the name of the documents field on a collection.
	CollectionDocumentsFieldName = "Documents"
)

// CollectionPath returns the path for the given collection.
func CollectionPath(collection string) datamodel.Path {
	return datamodel.ParsePath(RootCollectionsFieldName).AppendSegmentString(collection)
}

// DocumentsPath returns the path for the documents map of the given collection.
func DocumentsPath(collection string) datamodel.Path {
	return CollectionPath(collection).AppendSegmentString(CollectionDocumentsFieldName)
}

// DocumentPath returns the path for the document in the given collection with the given id.
func DocumentPath(collection string, id string) datamodel.Path {
	return DocumentsPath(collection).AppendSegmentString(id)
}

// BuildRootNode returns a new root node with the collections defined in the given schema.
func BuildRootNode(ctx context.Context, store *link.Store, schema string) (datamodel.Node, error) {
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
func BuildRootCollectionsNode(ctx context.Context, store *link.Store, s *ast.Schema) (datamodel.Node, error) {
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
