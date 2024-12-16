package core

import (
	"context"
	"errors"

	"github.com/fxamacker/cbor/v2"
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
	schema, err := schema_gen.Execute(string(schemaSource))
	if err != nil {
		return nil, err
	}
	repo := &Repository{
		schema:   schema,
		storage:  storage,
		conflict: TheirsConflictResolver,
	}
	collection := &CollectionRoot{
		Documents: make(map[string]Hash),
	}
	collectionHash, err := repo.CreateCollectionRoot(ctx, collection)
	if err != nil {
		return nil, err
	}
	collections := make(map[string]Hash)
	for _, t := range schema.Types {
		if t.BuiltIn || t.Kind != ast.Object {
			continue
		}
		collections[t.Name] = collectionHash
	}
	data := &DataRoot{
		Collections: collections,
	}
	dataHash, err := repo.CreateDataRoot(ctx, data)
	if err != nil {
		return nil, err
	}
	commit := &Commit{
		DataRoot: dataHash,
	}
	commitHash, err := repo.CreateCommit(ctx, commit)
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
	repo.head = commitHash
	return repo, nil
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

// Commit returns the commit with the given hash.
func (r *Repository) Commit(ctx context.Context, hash Hash) (*Commit, error) {
	data, err := r.storage.Get(ctx, hash.String())
	if err != nil {
		return nil, err
	}
	var commit Commit
	if err := cbor.Unmarshal(data, &commit); err != nil {
		return nil, err
	}
	return &commit, nil
}

// CreateCommit creates a new commit and returns its hash.
func (r *Repository) CreateCommit(ctx context.Context, commit *Commit) (Hash, error) {
	enc, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	data, err := enc.Marshal(commit)
	if err != nil {
		return nil, err
	}
	hash := Sum(data)
	if err := r.storage.Put(ctx, hash.String(), data); err != nil {
		return nil, err
	}
	return hash, nil
}

// DataRoot returns the data root with the given hash.
func (r *Repository) DataRoot(ctx context.Context, hash Hash) (*DataRoot, error) {
	data, err := r.storage.Get(ctx, hash.String())
	if err != nil {
		return nil, err
	}
	var dataRoot DataRoot
	if err := cbor.Unmarshal(data, &dataRoot); err != nil {
		return nil, err
	}
	return &dataRoot, nil
}

// CreateDataRoot creates a new data root and returns its hash.
func (r *Repository) CreateDataRoot(ctx context.Context, dataRoot *DataRoot) (Hash, error) {
	enc, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	data, err := enc.Marshal(dataRoot)
	if err != nil {
		return nil, err
	}
	hash := Sum(data)
	if err := r.storage.Put(ctx, hash.String(), data); err != nil {
		return nil, err
	}
	return hash, nil
}

// CollectionRoot returns the collection with the given hash.
func (r *Repository) CollectionRoot(ctx context.Context, hash Hash) (*CollectionRoot, error) {
	data, err := r.storage.Get(ctx, hash.String())
	if err != nil {
		return nil, err
	}
	var collectionsRoot CollectionRoot
	if err := cbor.Unmarshal(data, &collectionsRoot); err != nil {
		return nil, err
	}
	return &collectionsRoot, nil
}

// CreateCollectionRoot creates a collection and returns its hash.
func (r *Repository) CreateCollectionRoot(ctx context.Context, collectionRoot *CollectionRoot) (Hash, error) {
	enc, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	data, err := enc.Marshal(collectionRoot)
	if err != nil {
		return nil, err
	}
	hash := Sum(data)
	if err := r.storage.Put(ctx, hash.String(), data); err != nil {
		return nil, err
	}
	return hash, nil
}

// Document returns the document with the given hash.
func (r *Repository) Document(ctx context.Context, hash Hash) (map[string]any, error) {
	data, err := r.storage.Get(ctx, hash.String())
	if err != nil {
		return nil, err
	}
	var document map[string]any
	if err := cbor.Unmarshal(data, &document); err != nil {
		return nil, err
	}
	return document, nil
}

// CreateDocument creates a document and returns its hash.
func (r *Repository) CreateDocument(ctx context.Context, document map[string]any) (Hash, error) {
	enc, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	data, err := enc.Marshal(document)
	if err != nil {
		return nil, err
	}
	hash := Sum(data)
	if err := r.storage.Put(ctx, hash.String(), data); err != nil {
		return nil, err
	}
	return hash, nil
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
		collection, err := r.CollectionRoot(ctx, h)
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
