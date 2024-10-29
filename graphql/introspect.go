package graphql

import (
	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/introspection"
	"github.com/vektah/gqlparser/v2/ast"
)

func IsIntrospect(fields []graphql.CollectedField) bool {
	if len(fields) != 1 {
		return false
	}
	return fields[0].Name == "__schema" || fields[0].Name == "__type" || fields[0].Name == "__typename"
}

func Introspect(exec *execContext) map[string]any {
	fields := exec.collectFields(exec.operation.SelectionSet, []string{"Query"})
	out := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			out[field.Alias] = "Query"
		case "__type":
			out[field.Alias] = introspectQueryType(exec, field.Field)
		case "__schema":
			out[field.Alias] = introspectQuerySchema(exec, field.Field)
		}
	}
	return out
}

func introspectQueryType(exec *execContext, field *ast.Field) map[string]any {
	args := field.ArgumentMap(exec.variables)
	name := args["name"].(string)
	typ := introspection.WrapTypeFromDef(exec.schema, exec.schema.Types[name])
	return introspectType(exec, typ, field.SelectionSet)
}

func introspectQuerySchema(exec *execContext, field *ast.Field) map[string]any {
	typ := introspection.WrapSchema(exec.schema)
	return introspectSchema(exec, typ, field.SelectionSet)
}

func introspectSchema(exec *execContext, obj *introspection.Schema, sel ast.SelectionSet) map[string]any {
	fields := exec.collectFields(sel, []string{"__Schema"})
	out := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			out[field.Alias] = "__Schema"
		case "types":
			out[field.Alias] = introspectSchemaTypes(exec, field.SelectionSet)
		case "queryType":
			out[field.Alias] = introspectType(exec, obj.QueryType(), field.SelectionSet)
		case "mutationType":
			out[field.Alias] = introspectType(exec, obj.MutationType(), field.SelectionSet)
		case "subscriptionType":
			out[field.Alias] = introspectType(exec, obj.SubscriptionType(), field.SelectionSet)
		case "directives":
			out[field.Alias] = introspectDirectives(exec, obj.Directives(), field.SelectionSet)
		}
	}
	return out
}

func introspectDirectives(exec *execContext, obj []introspection.Directive, sel ast.SelectionSet) []any {
	out := make([]any, len(obj))
	for i, d := range obj {
		out[i] = introspectDirective(exec, d, sel)
	}
	return out
}

func introspectDirective(exec *execContext, obj introspection.Directive, sel ast.SelectionSet) map[string]any {
	fields := exec.collectFields(sel, []string{"__Directive"})
	out := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			out[field.Alias] = "__Directive"
		case "name":
			out[field.Alias] = obj.Name
		case "description":
			out[field.Alias] = obj.Description()
		case "locations":
			out[field.Alias] = obj.Locations
		case "args":
			out[field.Alias] = introspectInputValues(exec, obj.Args, field.SelectionSet)
		}
	}
	return out
}

func introspectSchemaTypes(exec *execContext, sel ast.SelectionSet) []any {
	out := make([]any, 0, len(exec.schema.Types))
	for _, t := range exec.schema.Types {
		out = append(out, introspectType(exec, introspection.WrapTypeFromDef(exec.schema, t), sel))
	}
	return out
}

func introspectTypes(exec *execContext, obj []introspection.Type, sel ast.SelectionSet) []any {
	out := make([]any, len(obj))
	for i, t := range obj {
		out[i] = introspectType(exec, &t, sel)
	}
	return out
}

func introspectType(exec *execContext, obj *introspection.Type, sel ast.SelectionSet) map[string]any {
	if obj == nil {
		return nil
	}
	fields := exec.collectFields(sel, []string{"__Type"})
	out := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			out[field.Alias] = "__Type"
		case "kind":
			out[field.Alias] = obj.Kind()
		case "name":
			out[field.Alias] = obj.Name()
		case "description":
			out[field.Alias] = obj.Description()
		case "fields":
			out[field.Alias] = introspectTypeFields(exec, obj, field.Field)
		case "interfaces":
			out[field.Alias] = introspectTypes(exec, obj.Interfaces(), field.SelectionSet)
		case "possibleTypes":
			out[field.Alias] = introspectTypes(exec, obj.PossibleTypes(), field.SelectionSet)
		case "enumValues":
			out[field.Alias] = introspectTypeEnumValues(exec, obj, field.Field)
		case "inputFields":
			out[field.Alias] = introspectInputValues(exec, obj.InputFields(), field.SelectionSet)
		case "ofType":
			out[field.Alias] = introspectType(exec, obj.OfType(), field.SelectionSet)
		}
	}
	return out
}

func introspectTypeFields(exec *execContext, typ *introspection.Type, field *ast.Field) []any {
	args := field.ArgumentMap(exec.variables)
	res := typ.Fields(args["includeDeprecated"].(bool))
	out := make([]any, len(res))
	for i, r := range res {
		out[i] = introspectField(exec, &r, field.SelectionSet)
	}
	return out
}

func introspectField(exec *execContext, obj *introspection.Field, sel ast.SelectionSet) map[string]any {
	fields := exec.collectFields(sel, []string{"__Field"})
	out := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			out[field.Alias] = "__Field"
		case "name":
			out[field.Alias] = obj.Name
		case "description":
			out[field.Alias] = obj.Description()
		case "args":
			out[field.Alias] = introspectInputValues(exec, obj.Args, field.SelectionSet)
		case "type":
			out[field.Alias] = introspectType(exec, obj.Type, field.SelectionSet)
		case "isDeprecated":
			out[field.Alias] = obj.IsDeprecated()
		case "deprecationReason":
			out[field.Alias] = obj.DeprecationReason()
		}
	}
	return out
}

func introspectInputValues(exec *execContext, obj []introspection.InputValue, sel ast.SelectionSet) []any {
	out := make([]any, len(obj))
	for i, o := range obj {
		out[i] = introspectInputValue(exec, o, sel)
	}
	return out
}

func introspectInputValue(exec *execContext, obj introspection.InputValue, sel ast.SelectionSet) map[string]any {
	fields := exec.collectFields(sel, []string{"__InputValue"})
	out := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			out[field.Alias] = "__InputValue"
		case "name":
			out[field.Alias] = obj.Name
		case "description":
			out[field.Alias] = obj.Description()
		case "type":
			out[field.Alias] = introspectType(exec, obj.Type, field.SelectionSet)
		case "defaultValue":
			out[field.Alias] = obj.DefaultValue
		}
	}
	return out
}

func introspectTypeEnumValues(exec *execContext, obj *introspection.Type, field *ast.Field) any {
	args := field.ArgumentMap(exec.variables)
	res := obj.EnumValues(args["includeDeprecated"].(bool))
	return introspectEnumValues(exec, res, field.SelectionSet)
}

func introspectEnumValues(exec *execContext, obj []introspection.EnumValue, sel ast.SelectionSet) any {
	out := make([]any, len(obj))
	for i, v := range obj {
		out[i] = introspectEnumValue(exec, v, sel)
	}
	return out
}

func introspectEnumValue(exec *execContext, obj introspection.EnumValue, sel ast.SelectionSet) map[string]any {
	fields := exec.collectFields(sel, []string{"__EnumValue"})
	out := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			out[field.Alias] = "__EnumValue"
		case "name":
			out[field.Alias] = obj.Name
		case "description":
			out[field.Alias] = obj.Description()
		case "isDeprecated":
			out[field.Alias] = obj.IsDeprecated()
		case "deprecationReason":
			out[field.Alias] = obj.DeprecationReason()
		}
	}
	return out
}
