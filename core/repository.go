package core

import (
	"context"
	"errors"

	"github.com/nasdf/capy/graphql/schema_gen"

	"github.com/vektah/gqlparser/v2/ast"
)

const (
	schemaKey = "schema"
	headKey   = "head"
)

type Repository struct {
	head     Hash
	schema   *ast.Schema
	storage  Storage
	conflict MergeConflictResolver
}

// InitRepository initializes a repo using the given schema and storage backend.
func InitRepository(ctx context.Context, storage Storage, schemaSource string) (*Repository, error) {
	schema, err := schema_gen.Execute(schemaSource)
	if err != nil {
		return nil, err
	}
	repo := Repository{
		storage: storage,
	}

	// create initial collection root
	collection := &Collection{
		Documents: make(map[string]Hash),
	}
	collectionHash, err := repo.CreateObject(ctx, collection)
	if err != nil {
		return nil, err
	}

	// create initial data root
	data := &DataRoot{
		Collections: make(map[string]Hash),
	}
	for _, t := range schema.Types {
		if !t.BuiltIn && t.Kind == ast.Object {
			data.Collections[t.Name] = collectionHash
		}
	}
	dataHash, err := repo.CreateObject(ctx, data)
	if err != nil {
		return nil, err
	}

	// create initial commit
	commit := &Commit{
		DataRoot: dataHash,
	}
	commitHash, err := repo.CreateObject(ctx, commit)
	if err != nil {
		return nil, err
	}

	err = storage.Put(ctx, headKey, commitHash)
	if err != nil {
		return nil, err
	}
	err = storage.Put(ctx, schemaKey, []byte(schemaSource))
	if err != nil {
		return nil, err
	}
	return OpenRepository(ctx, storage)
}

// OpenRepository returns an existing repo using the given storage backend.
func OpenRepository(ctx context.Context, storage Storage) (*Repository, error) {
	head, err := storage.Get(ctx, headKey)
	if !errors.Is(err, ErrNotFound) && err != nil {
		return nil, err
	}
	schemaSource, err := storage.Get(ctx, schemaKey)
	if err != nil {
		return nil, err
	}
	schema, err := schema_gen.Execute(string(schemaSource))
	if err != nil {
		return nil, err
	}
	return &Repository{
		head:     head,
		schema:   schema,
		storage:  storage,
		conflict: TheirsConflictResolver,
	}, nil
}

// Schema returns the schema that describes the collections.
func (r *Repository) Schema() *ast.Schema {
	return r.schema
}

// Head returns the repo head hash.
func (r *Repository) Head() Hash {
	return r.head
}

func (r *Repository) CreateObject(ctx context.Context, object Object) (Hash, error) {
	data, err := object.Encode()
	if err != nil {
		return nil, err
	}
	hash := Sum(data)
	if err := r.storage.Put(ctx, hash.String(), data); err != nil {
		return nil, err
	}
	return hash, nil
}

// Commit returns the commit with the given hash.
func (r *Repository) Commit(ctx context.Context, hash Hash) (*Commit, error) {
	data, err := r.storage.Get(ctx, hash.String())
	if err != nil {
		return nil, err
	}
	return DecodeCommit(data)
}

// DataRoot returns the data root with the given hash.
func (r *Repository) DataRoot(ctx context.Context, hash Hash) (*DataRoot, error) {
	data, err := r.storage.Get(ctx, hash.String())
	if err != nil {
		return nil, err
	}
	return DecodeDataRoot(data)
}

// Collection returns the collection with the given hash.
func (r *Repository) Collection(ctx context.Context, hash Hash) (*Collection, error) {
	data, err := r.storage.Get(ctx, hash.String())
	if err != nil {
		return nil, err
	}
	return DecodeCollection(data)
}

// Document returns the document with the given hash.
func (r *Repository) Document(ctx context.Context, hash Hash) (Document, error) {
	data, err := r.storage.Get(ctx, hash.String())
	if err != nil {
		return nil, err
	}
	return DecodeDocument(data)
}

// Dump returns a mapping of all collections and document ids in the repo.
//
// This function is primarily used for testing.
func (r *Repository) Dump(ctx context.Context) (map[string][]string, error) {
	commit, err := r.Commit(ctx, r.Head())
	if err != nil {
		return nil, err
	}
	dataRoot, err := r.DataRoot(ctx, commit.DataRoot)
	if err != nil {
		return nil, err
	}
	result := make(map[string][]string, len(dataRoot.Collections))
	for n, h := range dataRoot.Collections {
		collection, err := r.Collection(ctx, h)
		if err != nil {
			return nil, err
		}
		docs := make([]string, 0, len(collection.Documents))
		for id := range collection.Documents {
			docs = append(docs, id)
		}
		result[n] = docs
	}
	return result, nil
}
