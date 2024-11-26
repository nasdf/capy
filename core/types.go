package core

import (
	"fmt"
	"strings"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	RootTypeName        = "__Root"
	RootParentsTypeName = "__RootParents"
	RootLinkTypeName    = "__RootLink"

	RootSchemaFieldName  = "Schema"
	RootParentsFieldName = "Parents"

	LinkSuffix       = "Link"
	DocumentSuffix   = "+Document"
	CollectionSuffix = "+Collection"
)

var (
	TypeID                 = schema.SpawnString("ID")
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

// Prototype returns a NodePrototype for the given Node.
func Prototype(n datamodel.Node) datamodel.NodePrototype {
	tn, ok := n.(schema.TypedNode)
	if !ok {
		return basicnode.Prototype.Any
	}
	lnk, ok := tn.Type().(*schema.TypeLink)
	if ok && lnk.HasReferencedType() {
		return bindnode.Prototype(nil, lnk.ReferencedType())
	}
	return bindnode.Prototype(nil, tn.Type())
}

// SpawnTypeSystem creates an IPLD TypeSystem from the given GraphQL schema.
func SpawnTypeSystem(s *ast.Schema) (*schema.TypeSystem, []error) {
	types := []schema.Type{
		TypeID,
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

	var rootFields []schema.StructField
	for _, d := range s.Types {
		if d.BuiltIn {
			continue
		}
		switch d.Kind {
		case ast.Object:
			fields := make([]schema.StructField, len(d.Fields))
			for i, f := range d.Fields {
				fields[i] = schema.SpawnStructField(f.Name, strings.TrimSuffix(f.Type.String(), "!"), !f.Type.NonNull, !f.Type.NonNull)
			}
			types = append(types, schema.SpawnStruct(d.Name+DocumentSuffix, fields, schema.SpawnStructRepresentationMap(nil)))

			relationType := schema.SpawnString(d.Name)
			types = append(types, relationType)

			linkType := schema.SpawnLinkReference(d.Name+LinkSuffix, d.Name+DocumentSuffix)
			types = append(types, linkType)

			collectionType := schema.SpawnMap(d.Name+CollectionSuffix, TypeString.Name(), linkType.Name(), false)
			types = append(types, collectionType)

			types = append(types, schema.SpawnList(fmt.Sprintf("[%s]", d.Name), relationType.Name(), true))
			types = append(types, schema.SpawnList(fmt.Sprintf("[%s!]", d.Name), relationType.Name(), false))

			rootFields = append(rootFields, schema.SpawnStructField(d.Name, collectionType.Name(), false, false))

		case ast.Enum:
			members := make([]string, len(d.EnumValues))
			repr := make(schema.EnumRepresentation_String)
			for i, v := range d.EnumValues {
				members[i] = v.Name
				repr[v.Name] = v.Name
			}
			types = append(types, schema.SpawnEnum(d.Name, members, repr))
		}
	}
	rootLinkType := schema.SpawnLinkReference(RootLinkTypeName, RootTypeName)
	types = append(types, rootLinkType)

	rootParentsType := schema.SpawnList(RootParentsTypeName, rootLinkType.Name(), false)
	types = append(types, rootParentsType)

	rootFields = append(rootFields, schema.SpawnStructField(RootParentsFieldName, rootParentsType.Name(), false, false))
	rootFields = append(rootFields, schema.SpawnStructField(RootSchemaFieldName, TypeString.Name(), false, false))
	types = append(types, schema.SpawnStruct(RootTypeName, rootFields, schema.SpawnStructRepresentationMap(nil)))

	return schema.SpawnTypeSystem(types...)
}
