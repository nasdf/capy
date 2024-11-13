package types

import (
	"fmt"

	"github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	// RootTypeName is the name of the root struct type.
	RootTypeName        = "__Root"
	RootSchemaFieldName = "Schema"
	DocumentSuffix      = "Document"
	CollectionSuffix    = "Collection"
)

// baseTypes contains all of the scalar and list types.
var baseTypes = []schema.Type{
	schema.SpawnInt("Int"),
	schema.SpawnFloat("Float"),
	schema.SpawnBool("Boolean"),
	schema.SpawnString("String"),
	schema.SpawnList("[Int]", "Int", true),
	schema.SpawnList("[Int!]", "Int", false),
	schema.SpawnList("[Float]", "Float", true),
	schema.SpawnList("[Float!]", "Float", false),
	schema.SpawnList("[Boolean]", "Boolean", true),
	schema.SpawnList("[Boolean!]", "Boolean", false),
	schema.SpawnList("[String]", "String", true),
	schema.SpawnList("[String!]", "String", false),
}

func accumulate(s *ast.Schema, collections []string) *schema.TypeSystem {
	ts := schema.MustTypeSystem(baseTypes...)
	for _, d := range s.Types {
		if !d.BuiltIn {
			accumulateSchemaType(ts, d)
		}
	}
	rootFields := []schema.StructField{
		schema.SpawnStructField(RootSchemaFieldName, "String", false, false),
	}
	for _, n := range collections {
		collectionType := schema.SpawnMap(n+CollectionSuffix, "String", n, false)
		ts.Accumulate(collectionType)
		rootFields = append(rootFields, schema.SpawnStructField(n, collectionType.Name(), false, false))
	}
	ts.Accumulate(schema.SpawnStruct(RootTypeName, rootFields, schema.SpawnStructRepresentationMap(nil)))
	return ts
}

func accumulateSchemaType(ts *schema.TypeSystem, d *ast.Definition) {
	switch d.Kind {
	case ast.Object:
		accumulateSchemaStructType(ts, d)
	case ast.Enum:
		accumulateSchemaEnumType(ts, d)
	}
}

func accumulateSchemaEnumType(ts *schema.TypeSystem, d *ast.Definition) {
	members := make([]string, len(d.EnumValues))
	repr := make(schema.EnumRepresentation_String)
	for i, v := range d.EnumValues {
		members[i] = v.Name
		repr[v.Name] = v.Name
	}
	ts.Accumulate(schema.SpawnEnum(d.Name, members, repr))
}

func accumulateSchemaStructType(ts *schema.TypeSystem, d *ast.Definition) {
	fields := make([]schema.StructField, len(d.Fields))
	for i, field := range d.Fields {
		if field.Type.Elem != nil {
			fields[i] = schema.SpawnStructField(field.Name, field.Type.String(), !field.Type.Elem.NonNull, !field.Type.Elem.NonNull)
		} else {
			fields[i] = schema.SpawnStructField(field.Name, field.Type.String(), !field.Type.NonNull, !field.Type.NonNull)
		}
	}
	structType := schema.SpawnStruct(d.Name+DocumentSuffix, fields, schema.SpawnStructRepresentationMap(nil))
	ts.Accumulate(structType)

	linkType := schema.SpawnLinkReference(d.Name, structType.Name())
	ts.Accumulate(linkType)

	ts.Accumulate(schema.SpawnList(fmt.Sprintf("[%s]", d.Name), linkType.Name(), true))
	ts.Accumulate(schema.SpawnList(fmt.Sprintf("[%s!]", d.Name), linkType.Name(), false))
}
