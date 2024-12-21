package core

import (
	"context"
	"fmt"

	"github.com/rodent-software/capy/object"
)

// MergeConflictResolver is a callback function that is used to resolver merge conflicts.
type MergeConflictResolver func(ctx context.Context, base, ours, theirs any) (any, error)

// TheirsConflictResolver is a merge strategy that favors the changes labeled as theirs.
var TheirsConflictResolver MergeConflictResolver = func(ctx context.Context, base, ours, theirs any) (any, error) {
	return theirs, nil
}

// OursConflictResolver is a merge strategy that favors the changes labeled as ours.
var OursConflictResolver MergeConflictResolver = func(ctx context.Context, base, ours, theirs any) (any, error) {
	return ours, nil
}

// Merge attempts to Merge the commit with the given hash into the current head.
func (r *Repository) Merge(ctx context.Context, hash object.Hash) error {
	bases, err := r.mergeBase(ctx, r.head, hash)
	if err != nil {
		return err
	}
	if len(bases) == 0 {
		return fmt.Errorf("no merge base found")
	}
	head, err := r.mergeCommits(ctx, bases[0], r.head, hash)
	if err != nil {
		return err
	}
	r.head = head
	return nil
}

// mergeCommits returns the results of a three way merge between the given commit hashes.
func (r *Repository) mergeCommits(ctx context.Context, baseHash, ourHash, theirHash object.Hash) (object.Hash, error) {
	if theirHash.Equal(baseHash) {
		return ourHash, nil
	}
	if ourHash.Equal(baseHash) {
		return theirHash, nil
	}
	base, err := r.Commit(ctx, baseHash)
	if err != nil {
		return nil, err
	}
	ours, err := r.Commit(ctx, ourHash)
	if err != nil {
		return nil, err
	}
	theirs, err := r.Commit(ctx, theirHash)
	if err != nil {
		return nil, err
	}
	dataRoot, err := r.mergeDataRoots(ctx, base.DataRoot, ours.DataRoot, theirs.DataRoot)
	if err != nil {
		return nil, err
	}
	commit := &object.Commit{
		Parents:  []object.Hash{ourHash, theirHash},
		DataRoot: dataRoot,
	}
	return EncodeObject(ctx, r.storage, commit)
}

func (r *Repository) mergeDataRoots(ctx context.Context, baseHash, ourHash, theirHash object.Hash) (object.Hash, error) {
	if theirHash.Equal(baseHash) && ourHash.Equal(baseHash) {
		return baseHash, nil
	}
	if theirHash.Equal(baseHash) {
		return ourHash, nil
	}
	if ourHash.Equal(baseHash) {
		return theirHash, nil
	}
	base, err := r.DataRoot(ctx, baseHash)
	if err != nil {
		return nil, err
	}
	ours, err := r.DataRoot(ctx, ourHash)
	if err != nil {
		return nil, err
	}
	theirs, err := r.DataRoot(ctx, theirHash)
	if err != nil {
		return nil, err
	}
	keys := make(map[string]struct{})
	for k := range base.Collections {
		keys[k] = struct{}{}
	}
	for k := range ours.Collections {
		keys[k] = struct{}{}
	}
	for k := range theirs.Collections {
		keys[k] = struct{}{}
	}
	collections := make(map[string]object.Hash)
	for k := range keys {
		hash, err := r.mergeCollections(ctx, base.Collections[k], ours.Collections[k], theirs.Collections[k])
		if err != nil {
			return nil, err
		}
		collections[k] = hash
	}
	dataRoot := &object.DataRoot{
		Collections: collections,
	}
	return EncodeObject(ctx, r.storage, dataRoot)
}

func (r *Repository) mergeCollections(ctx context.Context, baseHash, ourHash, theirHash object.Hash) (object.Hash, error) {
	if theirHash.Equal(baseHash) && ourHash.Equal(baseHash) {
		return baseHash, nil
	}
	if theirHash.Equal(baseHash) {
		return ourHash, nil
	}
	if ourHash.Equal(baseHash) {
		return theirHash, nil
	}
	base, err := r.Collection(ctx, baseHash)
	if err != nil {
		return nil, err
	}
	ours, err := r.Collection(ctx, ourHash)
	if err != nil {
		return nil, err
	}
	theirs, err := r.Collection(ctx, theirHash)
	if err != nil {
		return nil, err
	}
	keys := make(map[string]struct{})
	for k := range base.Documents {
		keys[k] = struct{}{}
	}
	for k := range ours.Documents {
		keys[k] = struct{}{}
	}
	for k := range theirs.Documents {
		keys[k] = struct{}{}
	}
	documents := make(map[string]object.Hash)
	for k := range keys {
		hash, err := r.mergeDocuments(ctx, base.Documents[k], ours.Documents[k], theirs.Documents[k])
		if err != nil {
			return nil, err
		}
		documents[k] = hash
	}
	collection := &object.Collection{
		Documents: documents,
	}
	return EncodeObject(ctx, r.storage, collection)
}

func (r *Repository) mergeDocuments(ctx context.Context, baseHash, ourHash, theirHash object.Hash) (object.Hash, error) {
	if theirHash.Equal(baseHash) && ourHash.Equal(baseHash) {
		return baseHash, nil
	}
	if theirHash.Equal(baseHash) {
		return ourHash, nil
	}
	if ourHash.Equal(baseHash) {
		return theirHash, nil
	}
	base, err := r.Document(ctx, baseHash)
	if err != nil {
		return nil, err
	}
	ours, err := r.Document(ctx, ourHash)
	if err != nil {
		return nil, err
	}
	theirs, err := r.Document(ctx, theirHash)
	if err != nil {
		return nil, err
	}
	keys := make(map[string]struct{})
	for k := range base {
		keys[k] = struct{}{}
	}
	for k := range ours {
		keys[k] = struct{}{}
	}
	for k := range theirs {
		keys[k] = struct{}{}
	}
	document := object.NewDocument()
	for k := range keys {
		prop, err := r.mergeProperty(ctx, base[k], ours[k], theirs[k])
		if err != nil {
			return nil, err
		}
		document[k] = prop
	}
	return EncodeObject(ctx, r.storage, document)
}

func (r *Repository) mergeProperty(ctx context.Context, base, ours, theirs any) (any, error) {
	if theirs == base && ours == base {
		return base, nil
	}
	if theirs == base {
		return ours, nil
	}
	if ours == base {
		return theirs, nil
	}
	return r.conflict(ctx, base, ours, theirs)
}

// mergeBase returns the best common ancestor for merging the two given commits.
func (r *Repository) mergeBase(ctx context.Context, oldHash, newHash object.Hash) ([]object.Hash, error) {
	seen := map[string]struct{}{}
	iter := r.CommitIterator(newHash)
	for !iter.Done() {
		hash, _, err := iter.Next(ctx)
		if err != nil {
			return nil, err
		}
		if oldHash.Equal(hash) {
			return []object.Hash{hash}, nil
		}
		seen[hash.String()] = struct{}{}
	}
	var bases []object.Hash
	iter = r.CommitIterator(oldHash)
	for !iter.Done() {
		lnk, _, err := iter.Next(ctx)
		if err != nil {
			return nil, err
		}
		_, ok := seen[lnk.String()]
		if !ok {
			continue
		}
		bases = append(bases, lnk)
		iter.Skip()
	}
	return r.independents(ctx, bases)
}

// independents returns a sub list where each entry is not an ancestor of any other entry.
func (r *Repository) independents(ctx context.Context, hashes []object.Hash) ([]object.Hash, error) {
	if len(hashes) < 2 {
		return hashes, nil
	}
	keep := make(map[string]object.Hash)
	for _, h := range hashes {
		keep[h.String()] = h
	}
	seen := make(map[string]struct{})
	for _, h := range hashes {
		_, ok := keep[h.String()]
		if !ok {
			continue
		}
		iter := r.CommitIterator(h)
		for !iter.Done() && len(keep) > 1 {
			hash, _, err := iter.Next(ctx)
			if err != nil {
				return nil, err
			}
			_, ok := keep[hash.String()]
			if ok && !h.Equal(hash) {
				delete(keep, hash.String())
			}
			_, ok = seen[hash.String()]
			if ok {
				iter.Skip()
			}
			seen[hash.String()] = struct{}{}
		}
	}
	result := make([]object.Hash, 0, len(keep))
	for _, h := range keep {
		result = append(result, h)
	}
	return result, nil
}
