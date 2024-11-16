package types

import (
	"fmt"

	"github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	RootTypeName        = "__Root"
	RootSchemaFieldName = "Schema"
	LinkSuffix          = "Link"
	DocumentSuffix      = "+Document"
	CollectionSuffix    = "+Collection"
)

var (
	TypeInt                = schema.SpawnInt("Int")
	TypeFloat              = schema.SpawnFloat("Float")
	TypeBoolean            = schema.SpawnBool("Boolean")
	TypeString             = schema.SpawnString("String")
	TypeIntList            = schema.SpawnList("[Int]", TypeInt.Name(), true)
	TypeNotNullIntList     = schema.SpawnList("[Int!]", TypeInt.Name(), false)
	TypeFloatList          = schema.SpawnList("[Float]", TypeFloat.Name(), true)
	TypeNotNullFloatList   = schema.SpawnList("[Float!]", TypeFloat.Name(), false)
	TypeBooleanList        = schema.SpawnList("[Boolean]", TypeBoolean.Name(), true)
	TypeNotNullBooleanList = schema.SpawnList("[Boolean!]", TypeBoolean.Name(), false)
	TypeStringList         = schema.SpawnList("[String]", TypeString.Name(), true)
	TypeNotNullStringList  = schema.SpawnList("[String!]", TypeString.Name(), false)
)

var baseTypes = []schema.Type{
	TypeInt,
	TypeFloat,
	TypeBoolean,
	TypeString,
	TypeIntList,
	TypeNotNullIntList,
	TypeFloatList,
	TypeNotNullFloatList,
	TypeBooleanList,
	TypeNotNullBooleanList,
	TypeStringList,
	TypeNotNullStringList,
}

func accumulate(s *ast.Schema) *schema.TypeSystem {
	ts := schema.MustTypeSystem(baseTypes...)
	collections := make([]string, 0)
	for _, d := range s.Types {
		if d.BuiltIn {
			continue
		}
		if d.Kind == ast.Object {
			collections = append(collections, d.Name)
		}
		accumulateType(d, ts)
	}
	rootFields := []schema.StructField{
		schema.SpawnStructField(RootSchemaFieldName, "String", false, false),
	}
	for _, n := range collections {
		referenceType := schema.SpawnString(n)
		ts.Accumulate(referenceType)

		ts.Accumulate(schema.SpawnList(fmt.Sprintf("[%s]", referenceType.Name()), referenceType.Name(), true))
		ts.Accumulate(schema.SpawnList(fmt.Sprintf("[%s!]", referenceType.Name()), referenceType.Name(), false))

		linkType := schema.SpawnLinkReference(n+LinkSuffix, n+DocumentSuffix)
		ts.Accumulate(linkType)

		collectionType := schema.SpawnMap(n+CollectionSuffix, "String", linkType.Name(), false)
		ts.Accumulate(collectionType)

		rootFields = append(rootFields, schema.SpawnStructField(n, collectionType.Name(), false, false))
	}
	ts.Accumulate(schema.SpawnStruct(RootTypeName, rootFields, schema.SpawnStructRepresentationMap(nil)))
	return ts
}

func accumulateType(d *ast.Definition, ts *schema.TypeSystem) {
	switch d.Kind {
	case ast.Object:
		fields := make([]schema.StructField, len(d.Fields))
		for i, f := range d.Fields {
			fields[i] = schema.SpawnStructField(f.Name, f.Type.String(), !f.Type.NonNull, !f.Type.NonNull)
		}
		ts.Accumulate(schema.SpawnStruct(d.Name+DocumentSuffix, fields, schema.SpawnStructRepresentationMap(nil)))

	case ast.Enum:
		members := make([]string, len(d.EnumValues))
		repr := make(schema.EnumRepresentation_String)
		for i, v := range d.EnumValues {
			members[i] = v.Name
			repr[v.Name] = v.Name
		}
		ts.Accumulate(schema.SpawnEnum(d.Name, members, repr))
	}
}
