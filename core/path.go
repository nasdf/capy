package core

import "github.com/ipld/go-ipld-prime/datamodel"

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
