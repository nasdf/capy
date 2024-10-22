package plan

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
	ipldschema "github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal"
)

// Storage defines methods for storing and loading IPLD graphs.
type Storage interface {
	// GetRootLink returns the root link of the store.
	GetRootLink() datamodel.Link
	// SetRootLink sets the root link for the store.
	SetRootLink(lnk datamodel.Link)
	// Load returns the node identified by the given link.
	Load(ctx context.Context, lnk datamodel.Link) (datamodel.Node, error)
	// Store creates a node and returns the link used to identify it.
	Store(ctx context.Context, node datamodel.Node) (datamodel.Link, error)
	// Traversal returns a new traversal.Progress that can be used to traverse and IPLD graph.
	Traversal(ctx context.Context) traversal.Progress
	// TypeSystem returns the TypeSystem used to load and store data in the store.
	TypeSystem() *ipldschema.TypeSystem
}

// Node represents an operation to perform on an IPLD graph.
type Node interface {
	// Execute returns the results after running the Node operations.
	Execute(ctx context.Context, store Storage) (any, error)
}
