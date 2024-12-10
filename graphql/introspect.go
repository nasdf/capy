package graphql

import (
	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/introspection"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/vektah/gqlparser/v2/ast"
)

func (e *executionContext) introspectSchema(obj *introspection.Schema, sel ast.SelectionSet, na datamodel.NodeAssembler) error {
	fields := e.collectFields(sel, "__Schema")
	ma, err := na.BeginMap(int64(len(fields)))
	if err != nil {
		return err
	}
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString("__Schema")
			if err != nil {
				return err
			}

		case "types":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = e.introspectSchemaTypes(field.SelectionSet, va)
			if err != nil {
				return err
			}

		case "queryType":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = e.introspectType(obj.QueryType(), field.SelectionSet, va)
			if err != nil {
				return err
			}

		case "mutationType":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = e.introspectType(obj.MutationType(), field.SelectionSet, va)
			if err != nil {
				return err
			}

		case "subscriptionType":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = e.introspectType(obj.SubscriptionType(), field.SelectionSet, va)
			if err != nil {
				return err
			}

		case "directives":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = e.introspectDirectives(obj.Directives(), field.SelectionSet, va)
			if err != nil {
				return err
			}
		}
	}
	return ma.Finish()
}

func (e *executionContext) introspectType(obj *introspection.Type, sel ast.SelectionSet, na datamodel.NodeAssembler) error {
	fields := e.collectFields(sel, "__Type")
	if obj == nil {
		return na.AssignNull()
	}
	ma, err := na.BeginMap(int64(len(fields)))
	if err != nil {
		return err
	}
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString("__Type")
			if err != nil {
				return err
			}

		case "kind":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString(obj.Kind())
			if err != nil {
				return err
			}

		case "name":
			out := obj.Name()
			if out == nil {
				continue
			}
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString(*out)
			if err != nil {
				return err
			}

		case "description":
			out := obj.Description()
			if out == nil {
				continue
			}
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString(*out)
			if err != nil {
				return err
			}

		case "fields":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = e.introspectTypeFields(obj, field.Field, va)
			if err != nil {
				return err
			}

		case "interfaces":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = e.introspectTypes(obj.Interfaces(), field.SelectionSet, va)
			if err != nil {
				return err
			}

		case "possibleTypes":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = e.introspectTypes(obj.PossibleTypes(), field.SelectionSet, va)
			if err != nil {
				return err
			}

		case "enumValues":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = e.introspectTypeEnumValues(obj, field.Field, va)
			if err != nil {
				return err
			}

		case "inputFields":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = e.introspectInputValues(obj.InputFields(), field.SelectionSet, va)
			if err != nil {
				return err
			}

		case "ofType":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = e.introspectType(obj.OfType(), field.SelectionSet, va)
			if err != nil {
				return err
			}
		}
	}
	return ma.Finish()
}

func (e *executionContext) introspectField(obj *introspection.Field, sel ast.SelectionSet, na datamodel.NodeAssembler) error {
	fields := e.collectFields(sel, "__Field")
	ma, err := na.BeginMap(int64(len(fields)))
	if err != nil {
		return err
	}
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString("__Field")
			if err != nil {
				return err
			}

		case "name":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString(obj.Name)
			if err != nil {
				return err
			}

		case "description":
			out := obj.Description()
			if out == nil {
				continue
			}
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString(*out)
			if err != nil {
				return err
			}

		case "args":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = e.introspectInputValues(obj.Args, field.SelectionSet, va)
			if err != nil {
				return err
			}

		case "type":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = e.introspectType(obj.Type, field.SelectionSet, va)
			if err != nil {
				return err
			}

		case "isDeprecated":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignBool(obj.IsDeprecated())
			if err != nil {
				return err
			}

		case "deprecationReason":
			out := obj.DeprecationReason()
			if out == nil {
				continue
			}
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString(*out)
			if err != nil {
				return err
			}
		}
	}
	return ma.Finish()
}

func (e *executionContext) introspectInputValue(obj introspection.InputValue, sel ast.SelectionSet, na datamodel.NodeAssembler) error {
	fields := e.collectFields(sel, "__InputValue")
	ma, err := na.BeginMap(int64(len(fields)))
	if err != nil {
		return err
	}
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString("__InputValue")
			if err != nil {
				return err
			}

		case "name":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString(obj.Name)
			if err != nil {
				return err
			}

		case "description":
			out := obj.Description()
			if out == nil {
				continue
			}
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString(*out)
			if err != nil {
				return err
			}

		case "type":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = e.introspectType(obj.Type, field.SelectionSet, va)
			if err != nil {
				return err
			}

		case "defaultValue":
			out := obj.DefaultValue
			if out == nil {
				continue
			}
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString(*out)
			if err != nil {
				return err
			}
		}
	}
	return ma.Finish()
}

func (e *executionContext) introspectEnumValue(obj introspection.EnumValue, sel ast.SelectionSet, na datamodel.NodeAssembler) error {
	fields := e.collectFields(sel, "__EnumValue")
	ma, err := na.BeginMap(int64(len(fields)))
	if err != nil {
		return err
	}
	for _, field := range fields {

		switch field.Name {
		case "__typename":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString("__EnumValue")
			if err != nil {
				return err
			}

		case "name":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString(obj.Name)
			if err != nil {
				return err
			}

		case "description":
			out := obj.Description()
			if out == nil {
				continue
			}
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString(*out)
			if err != nil {
				return err
			}

		case "isDeprecated":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignBool(obj.IsDeprecated())
			if err != nil {
				return err
			}

		case "deprecationReason":
			out := obj.DeprecationReason()
			if out == nil {
				continue
			}
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString(*out)
			if err != nil {
				return err
			}
		}
	}
	return ma.Finish()
}

func (e *executionContext) introspectDirective(obj introspection.Directive, sel ast.SelectionSet, na datamodel.NodeAssembler) error {
	fields := e.collectFields(sel, "__Directive")
	ma, err := na.BeginMap(int64(len(fields)))
	if err != nil {
		return err
	}
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString("__Directive")
			if err != nil {
				return err
			}

		case "name":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString(obj.Name)
			if err != nil {
				return err
			}

		case "description":
			out := obj.Description()
			if out == nil {
				continue
			}
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = va.AssignString(*out)
			if err != nil {
				return err
			}

		case "locations":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			la, err := va.BeginList(int64(len(obj.Locations)))
			if err != nil {
				return err
			}
			for _, out := range obj.Locations {
				err = la.AssembleValue().AssignString(out)
				if err != nil {
					return err
				}
			}
			err = la.Finish()
			if err != nil {
				return err
			}

		case "args":
			va, err := ma.AssembleEntry(field.Alias)
			if err != nil {
				return err
			}
			err = e.introspectInputValues(obj.Args, field.SelectionSet, va)
			if err != nil {
				return err
			}
		}
	}
	return ma.Finish()
}

func (e *executionContext) introspectDirectives(obj []introspection.Directive, sel ast.SelectionSet, na datamodel.NodeAssembler) error {
	la, err := na.BeginList(int64(len(obj)))
	if err != nil {
		return err
	}
	for _, d := range obj {
		err = e.introspectDirective(d, sel, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func (e *executionContext) introspectSchemaTypes(sel ast.SelectionSet, na datamodel.NodeAssembler) error {
	la, err := na.BeginList(int64(len(e.store.Schema().Types)))
	if err != nil {
		return err
	}
	for _, t := range e.store.Schema().Types {
		err = e.introspectType(introspection.WrapTypeFromDef(e.store.Schema(), t), sel, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func (e *executionContext) introspectTypeEnumValues(obj *introspection.Type, field *ast.Field, na datamodel.NodeAssembler) error {
	args := field.ArgumentMap(e.params.Variables)
	vals := obj.EnumValues(args["includeDeprecated"].(bool))
	la, err := na.BeginList(int64(len(vals)))
	if err != nil {
		return err
	}
	for _, v := range vals {
		err = e.introspectEnumValue(v, field.SelectionSet, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func (e *executionContext) introspectInputValues(obj []introspection.InputValue, sel ast.SelectionSet, na datamodel.NodeAssembler) error {
	la, err := na.BeginList(int64(len(obj)))
	if err != nil {
		return err
	}
	for _, o := range obj {
		err = e.introspectInputValue(o, sel, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func (e *executionContext) introspectQuerySchema(field graphql.CollectedField, na datamodel.NodeAssembler) error {
	typ := introspection.WrapSchema(e.store.Schema())
	return e.introspectSchema(typ, field.SelectionSet, na)
}

func (e *executionContext) introspectQueryType(field graphql.CollectedField, na datamodel.NodeAssembler) error {
	args := field.ArgumentMap(e.params.Variables)
	name := args["name"].(string)
	typ := introspection.WrapTypeFromDef(e.store.Schema(), e.store.Schema().Types[name])
	return e.introspectType(typ, field.SelectionSet, na)
}

func (e *executionContext) introspectTypeFields(typ *introspection.Type, field *ast.Field, na datamodel.NodeAssembler) error {
	args := field.ArgumentMap(e.params.Variables)
	vals := typ.Fields(args["includeDeprecated"].(bool))
	la, err := na.BeginList(int64(len(vals)))
	if err != nil {
		return err
	}
	for _, v := range vals {
		err = e.introspectField(&v, field.SelectionSet, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func (e *executionContext) introspectTypes(obj []introspection.Type, sel ast.SelectionSet, na datamodel.NodeAssembler) error {
	la, err := na.BeginList(int64(len(obj)))
	if err != nil {
		return err
	}
	for _, t := range obj {
		err = e.introspectType(&t, sel, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}
