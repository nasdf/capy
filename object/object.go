package object

// Commit contains the state of the collections at point in time.
type Commit struct {
	// Parents is the list of parent commits this commit was created from.
	Parents []Hash
	// DataRoot is the hash of the data root.
	DataRoot Hash
}

// DataRoot is the root object for all data.
type DataRoot struct {
	// Collections is a mapping of names to collection root hashes.
	Collections map[string]Hash
}

// Collection is the root object for a collection.
type Collection struct {
	// Documents is a mapping of ids to document hashes.
	Documents map[string]Hash
}

type Document map[string]any

func NewDocument() Document {
	return make(map[string]any)
}
