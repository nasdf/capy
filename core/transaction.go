package core

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/rodent-software/capy/object"

	"github.com/google/uuid"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	// setPatch is a patch operation that sets a field value.
	setPatch = "set"
	// appendPatch is a patch operation that appends a value to a list field.
	appendPatch = "append"
	// filterPatch is a patch operation that filters a list value.
	filterPatch = "filter"
	// equalFilter matches if the target value is equal to the filter value.
	equalFilter = "eq"
	// notEqualFilter matches if the target value is not equal to the filter value.
	notEqualFilter = "neq"
	// greaterFilter matches if the target value is greater than the filter value.
	greaterFilter = "gt"
	// greaterOrEqualFilter matches if the target value is greater or equal to the filter value.
	greaterOrEqualFilter = "gte"
	// lessFilter matches if the target value is less than the filter value.
	lessFilter = "lt"
	// lessOrEqualFilter matches if the target value is less than or equal to the filter value.
	lessOrEqualFilter = "lte"
	// inFilter matches if the target value is included in the filter value list.
	inFilter = "in"
	// notInFilter matches if the target value is not included in the filter value list.
	notInFilter = "nin"
	// andFilter matches if all of the sub filters match.
	andFilter = "and"
	// orFilter matches if any of the sub filters match.
	orFilter = "or"
	// notFilter matches if the sub filter does not match.
	notFilter = "not"
	// allFilter matches if all of the target values match the sub filters.
	allFilter = "all"
	// allFilter matches if any of the target values match the sub filters.
	anyFilter = "any"
	// allFilter matches if none of the target values match the sub filters.
	noneFilter = "none"
)

// Transaction is used to create, read, and update documents.
type Transaction struct {
	repo *Repository
	data *object.DataRoot
	hash object.Hash
}

// Transactions returns a new transaction based on the commit with the given hash.
func (r *Repository) Transaction(ctx context.Context, hash object.Hash) (*Transaction, error) {
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
func (t *Transaction) Commit(ctx context.Context) (object.Hash, error) {
	data, err := EncodeObject(ctx, t.repo.storage, t.data)
	if err != nil {
		return nil, err
	}
	commit := &object.Commit{
		Parents:  []object.Hash{t.hash},
		DataRoot: data,
	}
	return EncodeObject(ctx, t.repo.storage, commit)
}

// ReadDocument returns the document from the given collection with the matching id.
func (t *Transaction) ReadDocument(ctx context.Context, collection, id string) (map[string]any, error) {
	colHash, ok := t.data.Collections[collection]
	if !ok {
		return nil, fmt.Errorf("collection does not exist: %s", collection)
	}
	col, err := t.repo.Collection(ctx, colHash)
	if err != nil {
		return nil, err
	}
	docHash, ok := col.Documents[id]
	if !ok {
		return nil, fmt.Errorf("document not found")
	}
	return t.repo.Document(ctx, docHash)
}

// DeleteDocument deletes the document from the given collection with the matching id.
func (t *Transaction) DeleteDocument(ctx context.Context, collection, id string) error {
	colHash, ok := t.data.Collections[collection]
	if !ok {
		return fmt.Errorf("collection does not exist: %s", collection)
	}
	col, err := t.repo.Collection(ctx, colHash)
	if err != nil {
		return err
	}
	delete(col.Documents, id)
	colHash, err = EncodeObject(ctx, t.repo.storage, col)
	if err != nil {
		return err
	}
	t.data.Collections[collection] = colHash
	return nil
}

// CreateDocument adds a document to the given collection and returns its unique id.
func (t *Transaction) CreateDocument(ctx context.Context, collection string, value map[string]any) (string, error) {
	def, ok := t.repo.schema.Types[collection]
	if !ok || def.BuiltIn || def.Kind != ast.Object {
		return "", fmt.Errorf("collection does not exist: %s", collection)
	}
	doc, err := t.createDocument(ctx, def, value)
	if err != nil {
		return "", err
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	docHash, err := EncodeObject(ctx, t.repo.storage, doc)
	if err != nil {
		return "", err
	}
	colHash, ok := t.data.Collections[collection]
	if !ok {
		return "", fmt.Errorf("collection does not exist: %s", collection)
	}
	col, err := t.repo.Collection(ctx, colHash)
	if err != nil {
		return "", err
	}
	col.Documents[id.String()] = docHash
	colHash, err = EncodeObject(ctx, t.repo.storage, col)
	if err != nil {
		return "", err
	}
	t.data.Collections[collection] = colHash
	return id.String(), nil
}

// FilterDocument returns a bool indicating if the document in the given collection with matching id passes the given filter.
func (t *Transaction) FilterDocument(ctx context.Context, collection, id string, filter any) (bool, error) {
	colHash, ok := t.data.Collections[collection]
	if !ok {
		return false, fmt.Errorf("collection does not exist: %s", collection)
	}
	col, err := t.repo.Collection(ctx, colHash)
	if err != nil {
		return false, err
	}
	docHash, ok := col.Documents[id]
	if !ok {
		return false, fmt.Errorf("document not found %s", id)
	}
	doc, err := t.repo.Document(ctx, docHash)
	if err != nil {
		return false, err
	}
	def := t.repo.schema.Types[collection]
	return t.filterDocument(ctx, def, doc, filter)
}

// PatchDocument updates the document in the given collection with matching id by applying the operations in the patch.
func (t *Transaction) PatchDocument(ctx context.Context, collection, id string, patch map[string]any) error {
	colHash, ok := t.data.Collections[collection]
	if !ok {
		return fmt.Errorf("collection does not exist: %s", collection)
	}
	col, err := t.repo.Collection(ctx, colHash)
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
	docHash, err = EncodeObject(ctx, t.repo.storage, doc)
	if err != nil {
		return err
	}
	col.Documents[id] = docHash
	colHash, err = EncodeObject(ctx, t.repo.storage, col)
	if err != nil {
		return err
	}
	t.data.Collections[collection] = colHash
	return nil
}

func (t *Transaction) createDocument(ctx context.Context, def *ast.Definition, value map[string]any) (object.Document, error) {
	out := make(map[string]any)
	for k, v := range value {
		field := def.Fields.ForName(k)
		if field == nil {
			return nil, fmt.Errorf("invalid document field %s", k)
		}
		norm, err := t.createValue(ctx, field.Type, v)
		if err != nil {
			return nil, err
		}
		out[k] = norm
	}
	return out, nil
}

func (t *Transaction) createValue(ctx context.Context, typ *ast.Type, value any) (any, error) {
	if !typ.NonNull && value == nil {
		return nil, nil
	}
	if typ.Elem != nil {
		return t.createList(ctx, typ.Elem, value.([]any))
	}
	def := t.repo.schema.Types[typ.NamedType]
	if def.Kind == ast.Object {
		return t.createRelation(ctx, typ, value.(map[string]any))
	}
	return value, nil
}

func (t *Transaction) createList(ctx context.Context, typ *ast.Type, value []any) ([]any, error) {
	out := make([]any, len(value))
	for i, v := range value {
		norm, err := t.createValue(ctx, typ, v)
		if err != nil {
			return nil, err
		}
		out[i] = norm
	}
	return out, nil
}

func (t *Transaction) createRelation(ctx context.Context, typ *ast.Type, value map[string]any) (string, error) {
	id, ok := value["id"].(string)
	if ok {
		return id, nil
	}
	return t.CreateDocument(ctx, typ.NamedType, value)
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
		res, err := t.patchValue(ctx, field.Type, v, p)
		if err != nil {
			return nil, err
		}
		out[field.Name] = res
	}
	return out, nil
}

func (t *Transaction) patchValue(ctx context.Context, typ *ast.Type, value any, patch any) (any, error) {
	if patch == nil {
		return value, nil
	}
	def, ok := t.repo.schema.Types[typ.NamedType]
	if ok && def.Kind == ast.Object {
		return t.patchRelation(ctx, typ, value, patch)
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
		return t.createValue(ctx, typ, p[op])
	case appendPatch:
		n, err := t.createValue(ctx, typ.Elem, p[op])
		if err != nil {
			return nil, err
		}
		if value == nil {
			return []any{n}, nil
		}
		return append(value.([]any), n), nil
	case filterPatch:
		result := make([]any, 0)
		for _, v := range value.([]any) {
			match, err := t.filterValue(ctx, typ.Elem, v, p[op])
			if err != nil {
				return nil, err
			}
			if match {
				result = append(result, v)
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("invalid patch operation %s", op)
	}
}

func (t *Transaction) patchRelation(ctx context.Context, typ *ast.Type, value any, patch any) (any, error) {
	if patch == nil {
		return nil, nil
	}
	err := t.PatchDocument(ctx, typ.NamedType, value.(string), patch.(map[string]any))
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (t *Transaction) filterDocument(ctx context.Context, def *ast.Definition, doc map[string]any, filter any) (bool, error) {
	if filter == nil {
		return true, nil
	}
	for key, val := range filter.(map[string]any) {
		switch key {
		case andFilter:
			match, err := t.filterAnd(ctx, def, doc, val)
			if err != nil || !match {
				return false, err
			}
		case orFilter:
			match, err := t.filterOr(ctx, def, doc, val)
			if err != nil || !match {
				return false, err
			}
		case notFilter:
			match, err := t.filterDocument(ctx, def, doc, val)
			if err != nil || match {
				return false, err
			}
		default:
			field := def.Fields.ForName(key)
			if field == nil {
				return false, fmt.Errorf("invalid document field %s", key)
			}
			match, err := t.filterValue(ctx, field.Type, doc[key], val)
			if err != nil || !match {
				return false, err
			}
		}
	}
	return true, nil
}

func (t *Transaction) filterValue(ctx context.Context, typ *ast.Type, value any, filter any) (bool, error) {
	if filter == nil {
		return true, nil
	}
	def := t.repo.schema.Types[typ.NamedType]
	if def.Kind == ast.Object {
		return t.filterRelation(ctx, typ, value, filter.(map[string]any))
	}
	for key, val := range filter.(map[string]any) {
		switch key {
		case equalFilter:
			match, err := filterEqual(value, val)
			if err != nil || !match {
				return false, err
			}
		case notEqualFilter:
			match, err := filterEqual(value, val)
			if err != nil || match {
				return false, err
			}
		case greaterFilter:
			match, err := filterCompare(value, val)
			if err != nil || match <= 0 {
				return false, err
			}
		case greaterOrEqualFilter:
			match, err := filterCompare(value, val)
			if err != nil || match < 0 {
				return false, err
			}
		case lessFilter:
			match, err := filterCompare(value, val)
			if err != nil || match >= 0 {
				return false, err
			}
		case lessOrEqualFilter:
			match, err := filterCompare(value, val)
			if err != nil || match > 0 {
				return false, err
			}
		case inFilter:
			match, err := filterIn(value, val)
			if err != nil || !match {
				return false, err
			}
		case notInFilter:
			match, err := filterIn(value, val)
			if err != nil || match {
				return false, err
			}
		case allFilter:
			match, err := t.filterAll(ctx, typ, value, val)
			if err != nil || !match {
				return false, err
			}
		case anyFilter:
			match, err := t.filterAny(ctx, typ, value, val)
			if err != nil || !match {
				return false, err
			}
		case noneFilter:
			match, err := t.filterAny(ctx, typ, value, val)
			if err != nil || match {
				return false, err
			}
		default:
			return false, fmt.Errorf("invalid filter operator %s", key)
		}
	}
	return true, nil
}

func (t *Transaction) filterRelation(ctx context.Context, typ *ast.Type, value any, filter map[string]any) (bool, error) {
	if filter == nil {
		return true, nil
	}
	doc, err := t.ReadDocument(ctx, typ.NamedType, value.(string))
	if err != nil {
		return false, err
	}
	def := t.repo.schema.Types[typ.NamedType]
	return t.filterDocument(ctx, def, doc, filter)
}

func (t *Transaction) filterAnd(ctx context.Context, def *ast.Definition, value map[string]any, filter any) (bool, error) {
	if filter == nil {
		return true, nil
	}
	for _, v := range filter.([]any) {
		match, err := t.filterDocument(ctx, def, value, v)
		if err != nil || !match {
			return false, err
		}
	}
	return true, nil
}

func (t *Transaction) filterOr(ctx context.Context, def *ast.Definition, value map[string]any, filter any) (bool, error) {
	if filter == nil {
		return true, nil
	}
	for _, v := range filter.([]any) {
		match, err := t.filterDocument(ctx, def, value, v)
		if err != nil || match {
			return match, err
		}
	}
	return true, nil
}

func (t *Transaction) filterAll(ctx context.Context, typ *ast.Type, value any, filter any) (bool, error) {
	if filter == nil {
		return true, nil
	}
	for _, v := range value.([]any) {
		match, err := t.filterValue(ctx, typ.Elem, v, filter)
		if err != nil || !match {
			return false, err
		}
	}
	return true, nil
}

func (t *Transaction) filterAny(ctx context.Context, typ *ast.Type, value any, filter any) (bool, error) {
	if filter == nil {
		return true, nil
	}
	for _, v := range value.([]any) {
		match, err := t.filterValue(ctx, typ.Elem, v, filter)
		if err != nil || match {
			return match, err
		}
	}
	return false, nil
}

func filterIn(value any, filter any) (bool, error) {
	switch v := value.(type) {
	case int64:
		return slices.Contains(filter.([]int64), v), nil
	case float64:
		return slices.Contains(filter.([]float64), v), nil
	case string:
		return slices.Contains(filter.([]string), v), nil
	default:
		return false, fmt.Errorf("invalid kind for in filter")
	}
}

func filterCompare(value any, filter any) (int, error) {
	switch v := value.(type) {
	case int64:
		return cmp.Compare(v, filter.(int64)), nil
	case float64:
		return cmp.Compare(v, filter.(float64)), nil
	case string:
		return cmp.Compare(v, filter.(string)), nil
	default:
		return 0, fmt.Errorf("invalid kind for compare filter")
	}
}

func filterEqual(value any, filter any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v == filter, nil
	default:
		match, err := filterCompare(v, filter)
		if err != nil {
			return false, err
		}
		return match == 0, nil
	}
}
