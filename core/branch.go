package core

import (
	"context"
	"fmt"

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

// Branch is a specific revision of the db that can be read, modified, and merged.
type Branch struct {
	store    *Store
	rootLink datamodel.Link
	rootNode datamodel.Node
}

// Branch creates a new branch from the given link.
func (s *Store) Branch(ctx context.Context, rootLink datamodel.Link) (*Branch, error) {
	rootNode, err := s.links.Load(ctx, rootLink, basicnode.Prototype.Map)
	if err != nil {
		return nil, err
	}
	return &Branch{
		store:    s,
		rootLink: rootLink,
		rootNode: rootNode,
	}, nil
}

// Commit creates a new commit containing the changes in this branch.
func (c *Branch) Commit(ctx context.Context) (datamodel.Link, error) {
	parentsNode, err := BuildRootParentsNode(c.rootLink)
	if err != nil {
		return nil, err
	}
	rootPath := datamodel.ParsePath(RootParentsFieldName)
	rootNode, err := c.store.links.SetNode(ctx, rootPath, c.rootNode, parentsNode)
	if err != nil {
		return nil, err
	}
	rootLink, err := c.store.links.Store(ctx, rootNode)
	if err != nil {
		return nil, err
	}
	c.rootLink = rootLink
	c.rootNode = rootNode
	return rootLink, nil
}

// Merge merges this branch into the main branch.
func (c *Branch) Merge(ctx context.Context) error {
	rootLink, err := c.Commit(ctx)
	if err != nil {
		return err
	}
	return c.store.Merge(ctx, rootLink)
}

// ReadDocument returns the document in the given collection with the given id.
func (c *Branch) ReadDocument(ctx context.Context, collection, id string) (datamodel.Node, error) {
	return c.store.links.GetNode(ctx, DocumentPath(collection, id), c.rootNode)
}

// DeleteDocument deletes the document in the given collection with the given id.
func (c *Branch) DeleteDocument(ctx context.Context, collection, id string) error {
	rootPath := DocumentPath(collection, id)
	rootNode, err := c.store.links.SetNode(ctx, rootPath, c.rootNode, nil)
	if err != nil {
		return err
	}
	c.rootNode = rootNode
	return nil
}

// CreateDocument creates a document in the given collection using the given value and returns its unique id.
func (c *Branch) CreateDocument(ctx context.Context, collection string, value map[string]any) (string, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	def, ok := c.store.schema.Types[collection]
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
	lnk, err := c.store.links.Store(ctx, nb.Build())
	if err != nil {
		return "", err
	}
	rootPath := DocumentPath(collection, id.String())
	rootNode, err := c.store.links.SetNode(ctx, rootPath, c.rootNode, basicnode.NewLink(lnk))
	if err != nil {
		return "", err
	}
	c.rootNode = rootNode
	return id.String(), nil
}

// PatchDocument patches the document in the given collection with the given id by applying the operations in the given value.
func (c *Branch) PatchDocument(ctx context.Context, collection, id string, value map[string]any) error {
	nb := basicnode.Prototype.Map.NewBuilder()
	n, err := c.ReadDocument(ctx, collection, id)
	if err != nil {
		return err
	}
	def, ok := c.store.schema.Types[collection]
	if !ok {
		return fmt.Errorf("invalid document type %s", collection)
	}
	err = c.patchObject(ctx, def, n, value, nb)
	if err != nil {
		return err
	}
	lnk, err := c.store.links.Store(ctx, nb.Build())
	if err != nil {
		return err
	}
	rootPath := DocumentPath(collection, id)
	rootNode, err := c.store.links.SetNode(ctx, rootPath, c.rootNode, basicnode.NewLink(lnk))
	if err != nil {
		return err
	}
	c.rootNode = rootNode
	return nil
}

func (c *Branch) assignObject(ctx context.Context, def *ast.Definition, value map[string]any, na datamodel.NodeAssembler) error {
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

func (c *Branch) assignValue(ctx context.Context, typ *ast.Type, value any, na datamodel.NodeAssembler) error {
	if !typ.NonNull && value == nil {
		return na.AssignNull()
	}
	if typ.Elem != nil {
		return c.assignList(ctx, typ.Elem, value.([]any), na)
	}
	def := c.store.schema.Types[typ.NamedType]
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

func (c *Branch) assignList(ctx context.Context, typ *ast.Type, value []any, na datamodel.NodeAssembler) error {
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

func (c *Branch) assignRelation(ctx context.Context, typ *ast.Type, value map[string]any, na datamodel.NodeAssembler) error {
	id, ok := value["id"].(string)
	if ok {
		return na.AssignString(id)
	}
	id, err := c.CreateDocument(ctx, typ.NamedType, value)
	if err != nil {
		return err
	}
	return na.AssignString(id)
}

func (c *Branch) patchObject(ctx context.Context, def *ast.Definition, n datamodel.Node, value map[string]any, na datamodel.NodeAssembler) error {
	ma, err := na.BeginMap(n.Length())
	if err != nil {
		return err
	}
	for _, field := range def.Fields {
		if field.Name == "link" || field.Name == "id" {
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

func (c *Branch) patchValue(ctx context.Context, typ *ast.Type, n datamodel.Node, value any, na datamodel.NodeAssembler) error {
	def, ok := c.store.schema.Types[typ.NamedType]
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

func (c *Branch) patchRelation(ctx context.Context, typ *ast.Type, n datamodel.Node, value map[string]any, na datamodel.NodeAssembler) error {
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

func (c *Branch) appendList(ctx context.Context, typ *ast.Type, n datamodel.Node, value any, na datamodel.NodeAssembler) error {
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
