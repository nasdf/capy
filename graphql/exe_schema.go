package graphql

import (
	"context"
	"fmt"
	"strings"

	"github.com/nasdf/capy/data"
	"github.com/nasdf/capy/node"
	"github.com/nasdf/capy/types"

	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/introspection"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

// QueryParams contains all of the parameters for a query.
type QueryParams struct {
	Query         string         `json:"query"`
	OperationName string         `json:"operationName"`
	Variables     map[string]any `json:"variables"`
}

// QueryResponse contains the fields expected from a GraphQL http response.
type QueryResponse struct {
	Data   any      `json:"data"`
	Errors []string `json:"errors,omitempty"`
}

type ExecutableSchema struct {
	schema  *ast.Schema
	store   *data.Store
	typeSys *schema.TypeSystem
}

type contextKey string

var (
	variablesContextKey = contextKey("variables")
	docContextKey       = contextKey("doc")
	rawQueryContextKey  = contextKey("rawQuery")
	linkContextKey      = contextKey("link")
	spanContextKey      = contextKey("span")
)

func NewExectuableSchema(typeSys *schema.TypeSystem, store *data.Store) (*ExecutableSchema, error) {
	input, err := GenerateSchema(typeSys)
	if err != nil {
		return nil, err
	}
	schema, err := gqlparser.LoadSchema(&ast.Source{Input: input})
	if err != nil {
		return nil, err
	}
	return &ExecutableSchema{
		typeSys: typeSys,
		store:   store,
		schema:  schema,
	}, nil
}

func (e *ExecutableSchema) Execute(ctx context.Context, rootLink datamodel.Link, params QueryParams) (any, datamodel.Link, error) {
	doc, errs := gqlparser.LoadQuery(e.schema, params.Query)
	if errs != nil {
		return nil, nil, errs
	}
	var operation *ast.OperationDefinition
	if params.OperationName != "" {
		operation = doc.Operations.ForName(params.OperationName)
	} else if len(doc.Operations) == 1 {
		operation = doc.Operations[0]
	}
	if operation == nil {
		return nil, nil, fmt.Errorf("operation is not defined")
	}

	ctx = context.WithValue(ctx, docContextKey, doc)
	ctx = context.WithValue(ctx, rawQueryContextKey, params.Query)
	ctx = context.WithValue(ctx, variablesContextKey, params.Variables)
	ctx = context.WithValue(ctx, linkContextKey, rootLink)

	switch operation.Operation {
	case ast.Mutation:
		return e.executeMutation(ctx, rootLink, operation.SelectionSet)
	case ast.Query:
		return e.executeQuery(ctx, rootLink, operation.SelectionSet)
	default:
		return nil, nil, fmt.Errorf("unsupported operation %s", operation.Operation)
	}
}

func (e *ExecutableSchema) executeMutation(ctx context.Context, rootLink datamodel.Link, set ast.SelectionSet) (map[string]any, datamodel.Link, error) {
	fields := e.collectFields(ctx, set, "Mutation")
	out := make(map[string]any)
	for _, field := range fields {
		switch {
		case strings.HasPrefix(field.Name, "create"):
			val, lnk, err := e.createMutation(ctx, rootLink, field)
			if err != nil {
				return nil, nil, err
			}
			rootLink = lnk
			out[field.Alias] = val

		default:
			return nil, nil, fmt.Errorf("unsupported operation %s", field.Name)
		}
	}
	return out, rootLink, nil
}

func (e *ExecutableSchema) createMutation(ctx context.Context, rootLink datamodel.Link, field graphql.CollectedField) (any, datamodel.Link, error) {
	args := field.ArgumentMap(ctx.Value(variablesContextKey).(map[string]any))
	collection := strings.TrimPrefix(field.Name, "create")

	builder := node.NewBuilder(e.store)
	_, err := builder.Build(ctx, e.typeSys.TypeByName(collection), args["input"])
	if err != nil {
		return nil, nil, err
	}

	rootType := e.typeSys.TypeByName(types.RootTypeName)
	rootNode, err := e.store.Load(ctx, rootLink, bindnode.Prototype(nil, rootType))
	if err != nil {
		return nil, nil, err
	}

	// append all of the objects that were created
	for col, links := range builder.Links() {
		for _, lnk := range links {
			path := datamodel.ParsePath(col).AppendSegmentString("-")
			rootNode, err = e.store.Traversal(ctx).FocusedTransform(rootNode, path, func(p traversal.Progress, n datamodel.Node) (datamodel.Node, error) {
				return basicnode.NewLink(lnk), nil
			}, true)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	// set the field name so we query the correct collection
	field.Name = collection
	// set the span so we only query the newly created object
	ctx = context.WithValue(ctx, spanContextKey, int64(-1))

	rootLink, err = e.store.Store(ctx, rootNode)
	if err != nil {
		return nil, nil, err
	}
	val, err := e.queryRoot(ctx, rootLink, field)
	if err != nil {
		return nil, nil, err
	}
	return val, rootLink, nil
}

func (e *ExecutableSchema) executeQuery(ctx context.Context, rootLink datamodel.Link, set ast.SelectionSet) (map[string]any, datamodel.Link, error) {
	fields := e.collectFields(ctx, set, "Query")
	out := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			out[field.Alias] = "Query"
		case "__type":
			out[field.Alias] = e.introspectQueryType(ctx, field)
		case "__schema":
			out[field.Alias] = e.introspectQuerySchema(ctx, field)
		default:
			res, err := e.queryRoot(ctx, rootLink, field)
			if err != nil {
				return nil, nil, err
			}
			out[field.Alias] = res
		}
	}
	return out, rootLink, nil
}

func (e *ExecutableSchema) queryRoot(ctx context.Context, rootLink datamodel.Link, field graphql.CollectedField) (any, error) {
	rootType := e.typeSys.TypeByName(types.RootTypeName)
	rootNode, err := e.store.Load(ctx, rootLink, bindnode.Prototype(nil, rootType))
	if err != nil {
		return nil, err
	}
	obj, err := rootNode.LookupByString(field.Name)
	if err != nil {
		return nil, err
	}
	val, err := e.queryField(ctx, obj, field)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (e *ExecutableSchema) queryField(ctx context.Context, n datamodel.Node, field graphql.CollectedField) (any, error) {
	if len(field.SelectionSet) == 0 {
		return node.Value(n)
	}
	switch n.Kind() {
	case datamodel.Kind_Link:
		return e.queryLink(ctx, n, field)
	case datamodel.Kind_List:
		return e.queryList(ctx, n, field)
	case datamodel.Kind_Map:
		return e.queryMap(ctx, n, field.SelectionSet)
	case datamodel.Kind_Null:
		return nil, nil
	default:
		return nil, fmt.Errorf("cannot traverse node of type %s", n.Kind().String())
	}
}

func (e *ExecutableSchema) queryLink(ctx context.Context, n datamodel.Node, field graphql.CollectedField) (any, error) {
	lnk, err := n.AsLink()
	if err != nil {
		return nil, err
	}
	obj, err := e.store.Load(ctx, lnk, basicnode.Prototype.Any)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, linkContextKey, lnk)
	return e.queryField(ctx, obj, field)
}

func (e *ExecutableSchema) queryList(ctx context.Context, n datamodel.Node, field graphql.CollectedField) ([]any, error) {
	span, hasSpan := ctx.Value(spanContextKey).(int64)
	ctx = context.WithValue(ctx, spanContextKey, nil)

	out := make([]any, 0, n.Length())
	iter := n.ListIterator()
	for !iter.Done() {
		i, obj, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if hasSpan && (span+n.Length()) != i {
			continue
		}
		match, err := e.queryFilter(ctx, obj, field)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		val, err := e.queryField(ctx, obj, field)
		if err != nil {
			return nil, err
		}
		out = append(out, val)
	}
	return out, nil
}

func (e *ExecutableSchema) queryFilter(ctx context.Context, n datamodel.Node, field graphql.CollectedField) (bool, error) {
	args := field.ArgumentMap(ctx.Value(variablesContextKey).(map[string]any))
	link, ok := args["link"].(string)
	if !ok {
		return true, nil
	}
	other, err := n.AsLink()
	if err != nil {
		return false, err
	}
	return link == other.String(), nil
}

func (e *ExecutableSchema) queryMap(ctx context.Context, n datamodel.Node, set ast.SelectionSet) (any, error) {
	out := make(map[string]any)
	fields := e.collectFields(ctx, set)
	for _, field := range fields {
		switch field.Name {
		case "_link":
			out[field.Alias] = ctx.Value(linkContextKey).(datamodel.Link).String()

		case "__typename":
			out[field.Alias] = "" // TODO n.(schema.TypedNode).Type().Name()

		default:
			obj, err := n.LookupByString(field.Name)
			if err != nil {
				return nil, err
			}
			val, err := e.queryField(ctx, obj, field)
			if err != nil {
				return nil, err
			}
			out[field.Alias] = val
		}
	}
	return out, nil
}

func (e *ExecutableSchema) introspectSchema(ctx context.Context, obj *introspection.Schema, sel ast.SelectionSet) map[string]any {
	fields := e.collectFields(ctx, sel, "__Schema")
	out := make(map[string]any)
	for _, field := range fields {
		switch field.Name {
		case "__typename":
			out[field.Alias] = "__Schema"
		case "types":
			out[field.Alias] = e.introspectSchemaTypes(ctx, field.SelectionSet)
		case "queryType":
			out[field.Alias] = e.introspectType(ctx, obj.QueryType(), field.SelectionSet)
		case "mutationType":
			out[field.Alias] = e.introspectType(ctx, obj.MutationType(), field.SelectionSet)
		case "subscriptionType":
			out[field.Alias] = e.introspectType(ctx, obj.SubscriptionType(), field.SelectionSet)
		case "directives":
			out[field.Alias] = e.introspectDirectives(ctx, obj.Directives(), field.SelectionSet)
		}
	}
	return out
}

func (e *ExecutableSchema) introspectType(ctx context.Context, obj *introspection.Type, sel ast.SelectionSet) map[string]any {
	if obj == nil {
		return nil
	}
	fields := e.collectFields(ctx, sel, "__Type")
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
			out[field.Alias] = e.introspectTypeFields(ctx, obj, field.Field)
		case "interfaces":
			out[field.Alias] = e.introspectTypes(ctx, obj.Interfaces(), field.SelectionSet)
		case "possibleTypes":
			out[field.Alias] = e.introspectTypes(ctx, obj.PossibleTypes(), field.SelectionSet)
		case "enumValues":
			out[field.Alias] = e.introspectTypeEnumValues(ctx, obj, field.Field)
		case "inputFields":
			out[field.Alias] = e.introspectInputValues(ctx, obj.InputFields(), field.SelectionSet)
		case "ofType":
			out[field.Alias] = e.introspectType(ctx, obj.OfType(), field.SelectionSet)
		}
	}
	return out
}

func (e *ExecutableSchema) introspectField(ctx context.Context, obj *introspection.Field, sel ast.SelectionSet) map[string]any {
	fields := e.collectFields(ctx, sel, "__Field")
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
			out[field.Alias] = e.introspectInputValues(ctx, obj.Args, field.SelectionSet)
		case "type":
			out[field.Alias] = e.introspectType(ctx, obj.Type, field.SelectionSet)
		case "isDeprecated":
			out[field.Alias] = obj.IsDeprecated()
		case "deprecationReason":
			out[field.Alias] = obj.DeprecationReason()
		}
	}
	return out
}

func (e *ExecutableSchema) introspectInputValue(ctx context.Context, obj introspection.InputValue, sel ast.SelectionSet) map[string]any {
	fields := e.collectFields(ctx, sel, "__InputValue")
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
			out[field.Alias] = e.introspectType(ctx, obj.Type, field.SelectionSet)
		case "defaultValue":
			out[field.Alias] = obj.DefaultValue
		}
	}
	return out
}

func (e *ExecutableSchema) introspectEnumValue(ctx context.Context, obj introspection.EnumValue, sel ast.SelectionSet) map[string]any {
	fields := e.collectFields(ctx, sel, "__EnumValue")
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

func (e *ExecutableSchema) introspectDirective(ctx context.Context, obj introspection.Directive, sel ast.SelectionSet) map[string]any {
	fields := e.collectFields(ctx, sel, "__Directive")
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
			out[field.Alias] = e.introspectInputValues(ctx, obj.Args, field.SelectionSet)
		}
	}
	return out
}

func (e *ExecutableSchema) introspectDirectives(ctx context.Context, obj []introspection.Directive, sel ast.SelectionSet) []any {
	out := make([]any, len(obj))
	for i, d := range obj {
		out[i] = e.introspectDirective(ctx, d, sel)
	}
	return out
}

func (e *ExecutableSchema) introspectSchemaTypes(ctx context.Context, sel ast.SelectionSet) []any {
	out := make([]any, 0, len(e.schema.Types))
	for _, t := range e.schema.Types {
		out = append(out, e.introspectType(ctx, introspection.WrapTypeFromDef(e.schema, t), sel))
	}
	return out
}

func (e *ExecutableSchema) introspectTypeEnumValues(ctx context.Context, obj *introspection.Type, field *ast.Field) []any {
	args := field.ArgumentMap(ctx.Value(variablesContextKey).(map[string]any))
	res := obj.EnumValues(args["includeDeprecated"].(bool))
	return e.introspectEnumValues(ctx, res, field.SelectionSet)
}

func (e *ExecutableSchema) introspectEnumValues(ctx context.Context, obj []introspection.EnumValue, sel ast.SelectionSet) []any {
	out := make([]any, len(obj))
	for i, v := range obj {
		out[i] = e.introspectEnumValue(ctx, v, sel)
	}
	return out
}

func (e *ExecutableSchema) introspectInputValues(ctx context.Context, obj []introspection.InputValue, sel ast.SelectionSet) []any {
	out := make([]any, len(obj))
	for i, o := range obj {
		out[i] = e.introspectInputValue(ctx, o, sel)
	}
	return out
}

func (e *ExecutableSchema) introspectQuerySchema(ctx context.Context, field graphql.CollectedField) map[string]any {
	typ := introspection.WrapSchema(e.schema)
	return e.introspectSchema(ctx, typ, field.SelectionSet)
}

func (e *ExecutableSchema) introspectQueryType(ctx context.Context, field graphql.CollectedField) map[string]any {
	args := field.ArgumentMap(ctx.Value(variablesContextKey).(map[string]any))
	name := args["name"].(string)
	typ := introspection.WrapTypeFromDef(e.schema, e.schema.Types[name])
	return e.introspectType(ctx, typ, field.SelectionSet)
}

func (e *ExecutableSchema) introspectTypeFields(ctx context.Context, typ *introspection.Type, field *ast.Field) []any {
	args := field.ArgumentMap(ctx.Value(variablesContextKey).(map[string]any))
	res := typ.Fields(args["includeDeprecated"].(bool))
	out := make([]any, len(res))
	for i, r := range res {
		out[i] = e.introspectField(ctx, &r, field.SelectionSet)
	}
	return out
}

func (e *ExecutableSchema) introspectTypes(ctx context.Context, obj []introspection.Type, sel ast.SelectionSet) []any {
	out := make([]any, len(obj))
	for i, t := range obj {
		out[i] = e.introspectType(ctx, &t, sel)
	}
	return out
}

func (e *ExecutableSchema) collectFields(ctx context.Context, sel ast.SelectionSet, satisfies ...string) []graphql.CollectedField {
	reqCtx := &graphql.OperationContext{
		RawQuery:  ctx.Value(rawQueryContextKey).(string),
		Variables: ctx.Value(variablesContextKey).(map[string]any),
		Doc:       ctx.Value(docContextKey).(*ast.QueryDocument),
	}
	return graphql.CollectFields(reqCtx, sel, satisfies)
}
