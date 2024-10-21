package query

import (
	"errors"
	"fmt"

	"github.com/nasdf/capy/schema"

	"github.com/99designs/gqlgen/graphql"
	ipldschema "github.com/ipld/go-ipld-prime/schema"
	"github.com/vektah/gqlparser/v2/ast"
)

// spawnResultType creates a new schema.Type for the results of the given selected fields.
func spawnResultType(ts *ipldschema.TypeSystem, fields []graphql.CollectedField) (ipldschema.Type, error) {
	var types []ipldschema.Type
	for _, t := range ts.GetTypes() {
		types = append(types, t)
	}
	resultTypeSys := ipldschema.MustTypeSystem(types...)

	rootType := ts.TypeByName(schema.RootTypeName).(*ipldschema.TypeStruct)
	resultName := "__Result"

	resultFields := make([]ipldschema.StructField, len(fields))
	for i, s := range fields {
		rootField := rootType.Field(s.Name)
		fieldName := fmt.Sprintf("%s_%s", resultName, s.Name)
		fieldType := accumulateResultType(fieldName, resultTypeSys, rootField.Type(), s.Field)
		resultFields[i] = ipldschema.SpawnStructField(rootField.Name(), fieldType, false, rootField.IsNullable())
	}
	resultTypeSys.Accumulate(ipldschema.SpawnStruct(resultName, resultFields, ipldschema.SpawnStructRepresentationMap(nil)))

	errs := resultTypeSys.ValidateGraph()
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return resultTypeSys.TypeByName(resultName), nil
}

func accumulateResultType(n string, ts *ipldschema.TypeSystem, t ipldschema.Type, f *ast.Field) string {
	switch v := t.(type) {
	case *ipldschema.TypeList:
		return fmt.Sprintf("[%s]", accumulateResultType(n, ts, v.ValueType(), f))
	case *ipldschema.TypeStruct:
		return accumulateResultStruct(n, ts, v, f)
	case *ipldschema.TypeLink:
		return accumulateResultLink(n, ts, v, f)
	default:
		return t.Name()
	}
}

func accumulateResultLink(n string, ts *ipldschema.TypeSystem, t *ipldschema.TypeLink, f *ast.Field) string {
	if !t.HasReferencedType() {
		return t.Name()
	}
	v, ok := t.ReferencedType().(*ipldschema.TypeStruct)
	if ok {
		return accumulateResultStruct(n, ts, v, f)
	}
	return t.ReferencedType().Name()
}

func accumulateResultStruct(n string, ts *ipldschema.TypeSystem, t *ipldschema.TypeStruct, f *ast.Field) string {
	fields := make([]ipldschema.StructField, len(f.SelectionSet))
	for i, s := range f.SelectionSet {
		selectField := s.(*ast.Field)
		structField := t.Field(selectField.Name)
		fieldName := fmt.Sprintf("%s_%s", n, selectField.Name)
		fieldType := accumulateResultType(fieldName, ts, structField.Type(), selectField)
		fields[i] = ipldschema.SpawnStructField(structField.Name(), fieldType, false, structField.IsNullable())
	}
	ts.Accumulate(ipldschema.SpawnStruct(n, fields, ipldschema.SpawnStructRepresentationMap(nil)))
	ts.Accumulate(ipldschema.SpawnList(fmt.Sprintf("[%s]", n), n, false))
	return n
}
