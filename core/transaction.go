package core

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	// setPatch is a patch operation that overwrites a field value.
	setPatch = "set"
	// appendPatch is a patch operation that appends a value to a list field.
	appendPatch = "append"
)

type Transaction struct {
	repo *Repository
	data *DataRoot
	hash Hash
}

// Transactions returns a new transaction based on the commit with the given hash.
func (r *Repository) Transaction(ctx context.Context, hash Hash) (*Transaction, error) {
	commit, err := r.Commit(ctx, hash)
	if err != nil {
		return nil, err
	}
	dataRoot, err := r.DataRoot(ctx, commit.DataRoot)
	if err != nil {
		return nil, err
	}
	return &Transaction{
		repo: r,
		data: dataRoot,
		hash: hash,
	}, nil
}

// Commit creates a new commit containing the transaction data.
func (t *Transaction) Commit(ctx context.Context) (Hash, error) {
	data, err := t.repo.CreateDataRoot(ctx, t.data)
	if err != nil {
		return nil, err
	}
	commit := &Commit{
		Parents:  []Hash{t.hash},
		DataRoot: data,
	}
	return t.repo.CreateCommit(ctx, commit)
}

func (t *Transaction) ReadDocument(ctx context.Context, collection, id string) (map[string]any, error) {
	colHash, ok := t.data.Collections[collection]
	if !ok {
		return nil, fmt.Errorf("collection does not exist: %s", collection)
	}
	col, err := t.repo.CollectionRoot(ctx, colHash)
	if err != nil {
		return nil, err
	}
	docHash, ok := col.Documents[id]
	if !ok {
		return nil, fmt.Errorf("document not found")
	}
	return t.repo.Document(ctx, docHash)
}

func (t *Transaction) DeleteDocument(ctx context.Context, collection, id string) error {
	colHash, ok := t.data.Collections[collection]
	if !ok {
		return fmt.Errorf("collection does not exist: %s", collection)
	}
	col, err := t.repo.CollectionRoot(ctx, colHash)
	if err != nil {
		return err
	}
	delete(col.Documents, id)
	colHash, err = t.repo.CreateCollectionRoot(ctx, col)
	if err != nil {
		return err
	}
	t.data.Collections[collection] = colHash
	return nil
}

func (t *Transaction) CreateDocument(ctx context.Context, collection string, value map[string]any) (string, error) {
	def, ok := t.repo.schema.Types[collection]
	if !ok || def.BuiltIn || def.Kind != ast.Object {
		return "", fmt.Errorf("collection does not exist: %s", collection)
	}
	doc, err := t.normalizeDocument(ctx, def, value)
	if err != nil {
		return "", err
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	docHash, err := t.repo.CreateDocument(ctx, doc)
	if err != nil {
		return "", err
	}
	colHash, ok := t.data.Collections[collection]
	if !ok {
		return "", fmt.Errorf("collection does not exist: %s", collection)
	}
	col, err := t.repo.CollectionRoot(ctx, colHash)
	if err != nil {
		return "", err
	}
	col.Documents[id.String()] = docHash
	colHash, err = t.repo.CreateCollectionRoot(ctx, col)
	if err != nil {
		return "", err
	}
	t.data.Collections[collection] = colHash
	return id.String(), nil
}

func (t *Transaction) PatchDocument(ctx context.Context, collection, id string, patch map[string]any) error {
	colHash, ok := t.data.Collections[collection]
	if !ok {
		return fmt.Errorf("collection does not exist: %s", collection)
	}
	col, err := t.repo.CollectionRoot(ctx, colHash)
	if err != nil {
		return err
	}
	docHash, ok := col.Documents[id]
	if !ok {
		return fmt.Errorf("document not found %s", id)
	}
	doc, err := t.repo.Document(ctx, docHash)
	if err != nil {
		return err
	}
	def := t.repo.schema.Types[collection]
	doc, err = t.patchDocument(ctx, def, doc, patch)
	if err != nil {
		return err
	}
	docHash, err = t.repo.CreateDocument(ctx, doc)
	if err != nil {
		return err
	}
	col.Documents[id] = docHash
	colHash, err = t.repo.CreateCollectionRoot(ctx, col)
	if err != nil {
		return err
	}
	t.data.Collections[collection] = colHash
	return nil
}

func (t *Transaction) patchDocument(ctx context.Context, def *ast.Definition, value map[string]any, patch map[string]any) (map[string]any, error) {
	out := make(map[string]any)
	for _, field := range def.Fields {
		if field.Name == "hash" || field.Name == "id" {
			continue // ignore system fields
		}
		v, hasValue := value[field.Name]
		p, hasPatch := patch[field.Name]
		if !hasValue && !hasPatch {
			continue // ignore empty fields
		}
		res, err := t.patchDocumentField(ctx, field.Type, v, p)
		if err != nil {
			return nil, err
		}
		out[field.Name] = res
	}
	return out, nil
}

func (t *Transaction) patchDocumentField(ctx context.Context, typ *ast.Type, value any, patch any) (any, error) {
	if patch == nil {
		return value, nil
	}
	def, ok := t.repo.schema.Types[typ.NamedType]
	if ok && def.Kind == ast.Object {
		return t.patchDocumentRelation(ctx, typ, value, patch)
	}
	p := patch.(map[string]any)
	if len(p) != 1 {
		return nil, fmt.Errorf("patch must contain exactly one operation")
	}
	var op string
	for k := range p {
		op = k
	}
	switch op {
	case setPatch:
		return p[op], nil
	case appendPatch:
		v, ok := value.([]any)
		if ok {
			return append(v, p[op]), nil
		}
		return []any{p[op]}, nil
	default:
		return nil, fmt.Errorf("invalid patch operation %s", op)
	}
}

func (t *Transaction) patchDocumentRelation(ctx context.Context, typ *ast.Type, value any, patch any) (any, error) {
	if patch == nil {
		return nil, nil
	}
	err := t.PatchDocument(ctx, typ.NamedType, value.(string), patch.(map[string]any))
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (t *Transaction) normalizeDocument(ctx context.Context, def *ast.Definition, value map[string]any) (map[string]any, error) {
	out := make(map[string]any)
	for k, v := range value {
		field := def.Fields.ForName(k)
		if field == nil {
			return nil, fmt.Errorf("invalid document field %s", k)
		}
		norm, err := t.normalizeDocumentField(ctx, field.Type, v)
		if err != nil {
			return nil, err
		}
		out[k] = norm
	}
	return out, nil
}

func (t *Transaction) normalizeDocumentField(ctx context.Context, typ *ast.Type, value any) (any, error) {
	if !typ.NonNull && value == nil {
		return nil, nil
	}
	if typ.Elem != nil {
		return t.normalizeDocumentList(ctx, typ.Elem, value.([]any))
	}
	def := t.repo.schema.Types[typ.NamedType]
	if def.Kind == ast.Object {
		return t.normalizeDocumentRelation(ctx, typ, value.(map[string]any))
	}
	return value, nil
}

func (t *Transaction) normalizeDocumentList(ctx context.Context, typ *ast.Type, value []any) ([]any, error) {
	out := make([]any, len(value))
	for i, v := range value {
		norm, err := t.normalizeDocumentField(ctx, typ, v)
		if err != nil {
			return nil, err
		}
		out[i] = norm
	}
	return out, nil
}

func (t *Transaction) normalizeDocumentRelation(ctx context.Context, typ *ast.Type, value map[string]any) (string, error) {
	id, ok := value["id"].(string)
	if ok {
		return id, nil
	}
	return t.CreateDocument(ctx, typ.NamedType, value)
}
