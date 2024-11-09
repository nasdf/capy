package types

import (
	"fmt"
	"slices"

	"github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	// RootTypeName is the name of the root struct type.
	RootTypeName        = "__Root"
	RootSchemaFieldName = "Schema"
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
			accumulateSchemaType(ts, d, collections)
		}
	}
	rootFields := []schema.StructField{
		schema.SpawnStructField(RootSchemaFieldName, "String", false, false),
	}
	for _, n := range collections {
		ts.Accumulate(schema.SpawnMap(n+CollectionSuffix, "String", "&"+n, false))
		rootFields = append(rootFields, schema.SpawnStructField(n, n+CollectionSuffix, false, false))
	}
	ts.Accumulate(schema.SpawnStruct(RootTypeName, rootFields, schema.SpawnStructRepresentationMap(nil)))
	return ts
}

func accumulateSchemaType(ts *schema.TypeSystem, d *ast.Definition, collections []string) {
	switch d.Kind {
	case ast.Object:
		accumulateSchemaStructType(ts, d, collections)
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

func accumulateSchemaStructType(ts *schema.TypeSystem, d *ast.Definition, collections []string) {
	fields := make([]schema.StructField, len(d.Fields))
	for i, field := range d.Fields {
		name := typeName(field.Type, collections)
		if field.Type.Elem != nil {
			fields[i] = schema.SpawnStructField(field.Name, name, !field.Type.Elem.NonNull, !field.Type.Elem.NonNull)
		} else {
			fields[i] = schema.SpawnStructField(field.Name, name, !field.Type.NonNull, !field.Type.NonNull)
		}
	}
	// accumulate object types
	ts.Accumulate(schema.SpawnStruct(d.Name, fields, schema.SpawnStructRepresentationMap(nil)))
	ts.Accumulate(schema.SpawnList(fmt.Sprintf("[%s]", d.Name), d.Name, true))
	ts.Accumulate(schema.SpawnList(fmt.Sprintf("[%s!]", d.Name), d.Name, false))
	// accumulate reference types
	ts.Accumulate(schema.SpawnLinkReference("&"+d.Name, d.Name))
	ts.Accumulate(schema.SpawnList(fmt.Sprintf("[&%s]", d.Name), "&"+d.Name, true))
	ts.Accumulate(schema.SpawnList(fmt.Sprintf("[&%s!]", d.Name), "&"+d.Name, false))
}

func typeName(t *ast.Type, collections []string) string {
	if t.Elem != nil {
		return fmt.Sprintf("[%s]", typeName(t.Elem, collections))
	}
	name := t.NamedType
	if slices.Contains(collections, name) {
		name = "&" + name
	}
	if t.NonNull {
		name = name + "!"
	}
	return name
}
