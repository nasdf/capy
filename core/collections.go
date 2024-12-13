package core

import (
	"context"
	"fmt"

	"github.com/nasdf/capy/link"

	"github.com/google/uuid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	// setPatch is a patch operation that overwrites a field value.
	setPatch = "set"
	// appendPatch is a patch operation that appends a value to a list field.
	appendPatch = "append"
)

// Collections contains all documents in the store.
type Collections struct {
	links    *link.Store
	schema   *ast.Schema
	rootLink datamodel.Link
	rootNode datamodel.Node
}

// Collections returns a collections root that can be used to create, read, update, and delete documents.
func (s *Store) Collections(ctx context.Context) (*Collections, error) {
	rootNode, err := s.links.Load(ctx, s.rootLink, basicnode.Prototype.Map)
	if err != nil {
		return nil, err
	}
	return &Collections{
		links:    s.links,
		schema:   s.schema,
		rootLink: s.rootLink,
		rootNode: rootNode,
	}, nil
}

// Commit creates a new root node containing the collections.
func (c *Collections) Commit(ctx context.Context) (datamodel.Link, error) {
	parentsNode, err := BuildRootParentsNode(c.rootLink)
	if err != nil {
		return nil, err
	}
	rootPath := datamodel.ParsePath(RootParentsFieldName)
	rootNode, err := c.links.SetNode(ctx, rootPath, c.rootNode, parentsNode)
	if err != nil {
		return nil, err
	}
	return c.links.Store(ctx, rootNode)
}

// ReadDocument returns the document in the given collection with the given id.
func (c *Collections) ReadDocument(ctx context.Context, collection, id string) (datamodel.Node, error) {
	return c.links.GetNode(ctx, DocumentPath(collection, id), c.rootNode)
}

// DeleteDocument deletes the document in the given collection with the given id.
func (c *Collections) DeleteDocument(ctx context.Context, collection, id string) error {
	rootPath := DocumentPath(collection, id)
	rootNode, err := c.links.SetNode(ctx, rootPath, c.rootNode, nil)
	if err != nil {
		return err
	}
	c.rootNode = rootNode
	return nil
}

// CreateDocument creates a document in the given collection using the given value and returns its unique id.
func (c *Collections) CreateDocument(ctx context.Context, collection string, value map[string]any) (string, error) {
	nb := basicnode.Prototype.Map.NewBuilder()

	def, ok := c.schema.Types[collection]
	if !ok {
		return "", fmt.Errorf("invalid document type %s", collection)
	}
	err := c.assignObject(ctx, def, value, nb)
	if err != nil {
		return "", err
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	lnk, err := c.links.Store(ctx, nb.Build())
	if err != nil {
		return "", err
	}
	rootPath := DocumentPath(collection, id.String())
	rootNode, err := c.links.SetNode(ctx, rootPath, c.rootNode, basicnode.NewLink(lnk))
	if err != nil {
		return "", err
	}
	c.rootNode = rootNode
	return id.String(), nil
}

// PatchDocument patches the document in the given collection with the given id by applying the operations in the given value.
func (c *Collections) PatchDocument(ctx context.Context, collection, id string, value map[string]any) error {
	nb := basicnode.Prototype.Map.NewBuilder()
	n, err := c.ReadDocument(ctx, collection, id)
	if err != nil {
		return err
	}
	def, ok := c.schema.Types[collection]
	if !ok {
		return fmt.Errorf("invalid document type %s", collection)
	}
	err = c.patchObject(ctx, def, n, value, nb)
	if err != nil {
		return err
	}
	lnk, err := c.links.Store(ctx, nb.Build())
	if err != nil {
		return err
	}
	rootPath := DocumentPath(collection, id)
	rootNode, err := c.links.SetNode(ctx, rootPath, c.rootNode, basicnode.NewLink(lnk))
	if err != nil {
		return err
	}
	c.rootNode = rootNode
	return nil
}

func (c *Collections) assignObject(ctx context.Context, def *ast.Definition, value map[string]any, na datamodel.NodeAssembler) error {
	ma, err := na.BeginMap(int64(len(value)))
	if err != nil {
		return err
	}
	for k, v := range value {
		field := def.Fields.ForName(k)
		if field == nil {
			return fmt.Errorf("invalid document field %s", k)
		}
		na, err := ma.AssembleEntry(field.Name)
		if err != nil {
			return err
		}
		err = c.assignValue(ctx, field.Type, v, na)
		if err != nil {
			return err
		}
	}
	return ma.Finish()
}

func (c *Collections) assignValue(ctx context.Context, typ *ast.Type, value any, na datamodel.NodeAssembler) error {
	if !typ.NonNull && value == nil {
		return na.AssignNull()
	}
	if typ.Elem != nil {
		return c.assignList(ctx, typ.Elem, value.([]any), na)
	}
	def := c.schema.Types[typ.NamedType]
	if def.Kind == ast.Object {
		return c.assignRelation(ctx, typ, value.(map[string]any), na)
	}
	switch typ.NamedType {
	case "String":
		return na.AssignString(value.(string))
	case "Boolean":
		return na.AssignBool(value.(bool))
	case "Int":
		return na.AssignInt(value.(int64))
	case "Float":
		return na.AssignFloat(value.(float64))
	default:
		return fmt.Errorf("invalid type %s", typ.NamedType)
	}
}

func (c *Collections) assignList(ctx context.Context, typ *ast.Type, value []any, na datamodel.NodeAssembler) error {
	la, err := na.BeginList(int64(len(value)))
	if err != nil {
		return err
	}
	for _, v := range value {
		err = c.assignValue(ctx, typ, v, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}

func (c *Collections) assignRelation(ctx context.Context, typ *ast.Type, value map[string]any, na datamodel.NodeAssembler) error {
	id, ok := value["_id"].(string)
	if ok {
		return na.AssignString(id)
	}
	id, err := c.CreateDocument(ctx, typ.NamedType, value)
	if err != nil {
		return err
	}
	return na.AssignString(id)
}

func (c *Collections) patchObject(ctx context.Context, def *ast.Definition, n datamodel.Node, value map[string]any, na datamodel.NodeAssembler) error {
	ma, err := na.BeginMap(n.Length())
	if err != nil {
		return err
	}
	for _, field := range def.Fields {
		if field.Name == "_link" || field.Name == "_id" {
			continue // ignore system fields
		}
		nv, err := n.LookupByString(field.Name)
		if _, ok := err.(datamodel.ErrNotExists); err != nil && !ok {
			return err
		}
		patch, ok := value[field.Name]
		if !ok && nv == nil {
			continue // ignore empty fields
		}
		na, err := ma.AssembleEntry(field.Name)
		if err != nil {
			return err
		}
		if ok {
			err = c.patchValue(ctx, field.Type, nv, patch, na)
		} else {
			err = na.AssignNode(nv)
		}
		if err != nil {
			return err
		}
	}
	return ma.Finish()
}

func (c *Collections) patchValue(ctx context.Context, typ *ast.Type, n datamodel.Node, value any, na datamodel.NodeAssembler) error {
	def, ok := c.schema.Types[typ.NamedType]
	if ok && def.Kind == ast.Object {
		return c.patchRelation(ctx, typ, n, value.(map[string]any), na)
	}
	patch := value.(map[string]any)
	if len(patch) != 1 {
		return fmt.Errorf("patch must contain exactly one operation")
	}
	var op string
	for k := range patch {
		op = k
	}
	switch op {
	case setPatch:
		return c.assignValue(ctx, typ, patch[op], na)
	case appendPatch:
		return c.appendList(ctx, typ, n, patch[op], na)
	default:
		return fmt.Errorf("invalid patch operation %s", op)
	}
}

func (c *Collections) patchRelation(ctx context.Context, typ *ast.Type, n datamodel.Node, value map[string]any, na datamodel.NodeAssembler) error {
	if n == nil {
		return na.AssignNull()
	}
	id, err := n.AsString()
	if err != nil {
		return err
	}
	err = c.PatchDocument(ctx, typ.NamedType, id, value)
	if err != nil {
		return err
	}
	return na.AssignString(id)
}

func (c *Collections) appendList(ctx context.Context, typ *ast.Type, n datamodel.Node, value any, na datamodel.NodeAssembler) error {
	vals, ok := value.([]any)
	if !ok {
		vals = append(vals, value)
	}
	if n == nil {
		return c.assignList(ctx, typ.Elem, vals, na)
	}
	la, err := na.BeginList(n.Length() + int64(len(vals)))
	if err != nil {
		return err
	}
	iter := n.ListIterator()
	for iter != nil && !iter.Done() {
		_, v, err := iter.Next()
		if err != nil {
			return err
		}
		err = la.AssembleValue().AssignNode(v)
		if err != nil {
			return err
		}
	}
	for _, v := range vals {
		err = c.assignValue(ctx, typ.Elem, v, la.AssembleValue())
		if err != nil {
			return err
		}
	}
	return la.Finish()
}
