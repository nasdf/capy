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
	TypeID                 = schema.SpawnString("ID")
	TypeNotNullID          = schema.SpawnString("ID!")
	TypeInt                = schema.SpawnInt("Int")
	TypeNotNullInt         = schema.SpawnInt("Int!")
	TypeFloat              = schema.SpawnFloat("Float")
	TypeNotNullFloat       = schema.SpawnFloat("Float!")
	TypeBoolean            = schema.SpawnBool("Boolean")
	TypeNotNullBoolean     = schema.SpawnBool("Boolean!")
	TypeString             = schema.SpawnString("String")
	TypeNotNullString      = schema.SpawnString("String!")
	TypeIntList            = schema.SpawnList("[Int]", TypeInt.Name(), true)
	TypeNotNullIntList     = schema.SpawnList("[Int!]", TypeInt.Name(), false)
	TypeFloatList          = schema.SpawnList("[Float]", TypeFloat.Name(), true)
	TypeNotNullFloatList   = schema.SpawnList("[Float!]", TypeFloat.Name(), false)
	TypeBooleanList        = schema.SpawnList("[Boolean]", TypeBoolean.Name(), true)
	TypeNotNullBooleanList = schema.SpawnList("[Boolean!]", TypeBoolean.Name(), false)
	TypeStringList         = schema.SpawnList("[String]", TypeString.Name(), true)
	TypeNotNullStringList  = schema.SpawnList("[String!]", TypeString.Name(), false)
)

func defaultTypeSystem() *schema.TypeSystem {
	return schema.MustTypeSystem(
		TypeID,
		TypeNotNullID,
		TypeInt,
		TypeNotNullInt,
		TypeFloat,
		TypeNotNullFloat,
		TypeBoolean,
		TypeNotNullBoolean,
		TypeString,
		TypeNotNullString,
		TypeIntList,
		TypeNotNullIntList,
		TypeFloatList,
		TypeNotNullFloatList,
		TypeBooleanList,
		TypeNotNullBooleanList,
		TypeStringList,
		TypeNotNullStringList,
	)
}

func schemaTypeSystem(s *ast.Schema) *schema.TypeSystem {
	rootFields := []schema.StructField{
		schema.SpawnStructField(RootSchemaFieldName, "String", false, false),
	}
	ts := defaultTypeSystem()
	for _, d := range s.Types {
		if d.BuiltIn {
			continue
		}
		switch d.Kind {
		case ast.Object:
			fields := make([]schema.StructField, len(d.Fields))
			for i, f := range d.Fields {
				fields[i] = schema.SpawnStructField(f.Name, f.Type.String(), !f.Type.NonNull, !f.Type.NonNull)
			}
			ts.Accumulate(schema.SpawnStruct(d.Name+DocumentSuffix, fields, schema.SpawnStructRepresentationMap(nil)))

			relationType := schema.SpawnString(d.Name)
			ts.Accumulate(relationType)

			linkType := schema.SpawnLinkReference(d.Name+LinkSuffix, d.Name+DocumentSuffix)
			ts.Accumulate(linkType)

			collectionType := schema.SpawnMap(d.Name+CollectionSuffix, "String", linkType.Name(), false)
			ts.Accumulate(collectionType)

			ts.Accumulate(schema.SpawnList(fmt.Sprintf("[%s]", relationType.Name()), relationType.Name(), true))
			ts.Accumulate(schema.SpawnList(fmt.Sprintf("[%s!]", relationType.Name()), relationType.Name(), false))

			rootFields = append(rootFields, schema.SpawnStructField(d.Name, collectionType.Name(), false, false))

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
	ts.Accumulate(schema.SpawnStruct(RootTypeName, rootFields, schema.SpawnStructRepresentationMap(nil)))
	return ts
}
