package core

import (
	"bytes"
	"context"
	"errors"

	"github.com/nasdf/capy/codec"
	"github.com/nasdf/capy/graphql/schema_gen"
	"github.com/nasdf/capy/object"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	//SchemaKey is the key used to store the input schema.
	SchemaKey = "schema"
	// HeadKey is the key used to store the repo head.
	HeadKey = "head"
)

// Repository contains all database objects.
type Repository struct {
	head     object.Hash
	schema   *ast.Schema
	storage  Storage
	conflict MergeConflictResolver
}

func NewRepository(head object.Hash, schemaInput string, storage Storage) (*Repository, error) {
	schema, err := schema_gen.Execute(schemaInput)
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

// InitRepository initializes a repo using the given schema and storage backend.
func InitRepository(ctx context.Context, storage Storage, schemaInput string) (*Repository, error) {
	schema, err := gqlparser.LoadSchema(&ast.Source{Input: schemaInput})
	if err != nil {
		return nil, err
	}

	// create initial collection root
	collection := &object.Collection{
		Documents: make(map[string]object.Hash),
	}
	collectionHash, err := EncodeObject(ctx, storage, collection)
	if err != nil {
		return nil, err
	}

	// create initial data root
	data := &object.DataRoot{
		Collections: make(map[string]object.Hash),
	}
	for _, t := range schema.Types {
		if !t.BuiltIn && t.Kind == ast.Object {
			data.Collections[t.Name] = collectionHash
		}
	}
	dataHash, err := EncodeObject(ctx, storage, data)
	if err != nil {
		return nil, err
	}

	// create initial commit
	commit := &object.Commit{
		DataRoot: dataHash,
	}
	commitHash, err := EncodeObject(ctx, storage, commit)
	if err != nil {
		return nil, err
	}

	err = storage.Put(ctx, HeadKey, commitHash)
	if err != nil {
		return nil, err
	}
	err = storage.Put(ctx, SchemaKey, []byte(schemaInput))
	if err != nil {
		return nil, err
	}
	return NewRepository(commitHash, schemaInput, storage)
}

// OpenRepository returns an existing repo using the given storage backend.
func OpenRepository(ctx context.Context, storage Storage) (*Repository, error) {
	head, err := storage.Get(ctx, HeadKey)
	if !errors.Is(err, ErrNotFound) && err != nil {
		return nil, err
	}
	schemaInput, err := storage.Get(ctx, SchemaKey)
	if err != nil {
		return nil, err
	}
	return NewRepository(head, string(schemaInput), storage)
}

// Schema returns the schema that describes the collections.
func (r *Repository) Schema() *ast.Schema {
	return r.schema
}

// Head returns the repo head hash.
func (r *Repository) Head() object.Hash {
	return r.head
}

// Commit returns the commit with the given hash.
func (r *Repository) Commit(ctx context.Context, hash object.Hash) (*object.Commit, error) {
	data, err := r.storage.Get(ctx, hash.String())
	if err != nil {
		return nil, err
	}
	dec := codec.NewDecoder(bytes.NewBuffer(data))
	return dec.DecodeCommit()
}

// DataRoot returns the data root with the given hash.
func (r *Repository) DataRoot(ctx context.Context, hash object.Hash) (*object.DataRoot, error) {
	data, err := r.storage.Get(ctx, hash.String())
	if err != nil {
		return nil, err
	}
	dec := codec.NewDecoder(bytes.NewBuffer(data))
	return dec.DecodeDataRoot()
}

// Collection returns the collection with the given hash.
func (r *Repository) Collection(ctx context.Context, hash object.Hash) (*object.Collection, error) {
	data, err := r.storage.Get(ctx, hash.String())
	if err != nil {
		return nil, err
	}
	dec := codec.NewDecoder(bytes.NewBuffer(data))
	return dec.DecodeCollection()
}

// Document returns the document with the given hash.
func (r *Repository) Document(ctx context.Context, hash object.Hash) (object.Document, error) {
	data, err := r.storage.Get(ctx, hash.String())
	if err != nil {
		return nil, err
	}
	dec := codec.NewDecoder(bytes.NewBuffer(data))
	return dec.DecodeDocument()
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
