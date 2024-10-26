package plan

import (
	"context"
)

type queryNode struct {
	// sel contains the fields that will be selected.
	req Request
}

// Query returns a new node that returns the selected fields when executed.
func Query(req Request) Node {
	return &queryNode{
		req: req,
	}
}

func (n *queryNode) Execute(ctx context.Context, p *Planner) (any, error) {
	return p.query(ctx, n.req)
}
