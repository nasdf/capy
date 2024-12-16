package core

import (
	"context"
	"fmt"
	"slices"
)

type CommitIterator struct {
	repo *Repository
	next []Hash
	seen map[string]struct{}
	prev int
}

// CommitIterator returns a new iterator that can be used to iterate through all parents of a commit.
func (r *Repository) CommitIterator(hash Hash) *CommitIterator {
	return &CommitIterator{
		repo: r,
		next: []Hash{hash},
		seen: make(map[string]struct{}),
	}
}

// CommitIterator returns a new iterator that can be used to iterate through all parents of a commit.
func (t *Transaction) CommitIterator() *CommitIterator {
	return &CommitIterator{
		repo: t.repo,
		next: []Hash{t.hash},
		seen: make(map[string]struct{}),
	}
}

// Done returns true if the iterator has no items left.
func (i *CommitIterator) Done() bool {
	return len(i.next) == 0
}

// Skip skips the parents of the last node visited by the iterator.
func (i *CommitIterator) Skip() {
	i.next = i.next[:i.prev]
	i.prev = len(i.next)
}

// Next returns the next commit from the iterator.
func (i *CommitIterator) Next(ctx context.Context) (Hash, *Commit, error) {
	hash := i.next[0]
	i.next = i.next[1:]
	i.prev = len(i.next)

	commit, err := i.repo.Commit(ctx, hash)
	if err != nil {
		return nil, nil, err
	}
	for _, p := range commit.Parents {
		_, ok := i.seen[p.String()]
		if ok {
			continue
		}
		i.seen[p.String()] = struct{}{}
		i.next = append(i.next, p)
	}
	return hash, commit, nil
}

// DocumentIterator iterates over all documents in a collection.
type DocumentIterator struct {
	repo *Repository
	keys []string
	docs map[string]Hash
}

// NewDocumentIterator returns a new iterator that can be used to iterate through all documents in a collection.
func (t *Transaction) DocumentIterator(ctx context.Context, collection string) (*DocumentIterator, error) {
	hash, ok := t.data.Collections[collection]
	if !ok {
		return nil, fmt.Errorf("collection does not exist: %s", collection)
	}
	col, err := t.repo.CollectionRoot(ctx, hash)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(col.Documents))
	for k := range col.Documents {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return &DocumentIterator{
		repo: t.repo,
		keys: keys,
		docs: col.Documents,
	}, nil
}

// Done returns true if the iterator has no items left.
func (i *DocumentIterator) Done() bool {
	return len(i.keys) == 0
}

// Next returns the next document id and document node from the iterator.
func (i *DocumentIterator) Next(ctx context.Context) (string, Hash, map[string]any, error) {
	key := i.keys[0]
	val := i.docs[key]
	i.keys = i.keys[1:]

	doc, err := i.repo.Document(ctx, val)
	if err != nil {
		return "", nil, nil, err
	}
	return key, val, doc, nil
}
