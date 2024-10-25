package plan

import (
	"context"

	"github.com/ipld/go-ipld-prime/datamodel"
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

func (n *queryNode) Execute(ctx context.Context, p *Planner) (*Result, error) {
	for i, f := range n.req.Fields {
		id, ok := f.Arguments["id"]
		if !ok {
			continue
		}
		index, err := p.findIndex(ctx, f.Name, id.(datamodel.Link))
		if err != nil {
			return nil, err
		}
		n.req.Fields[i].Arguments["id"] = index
	}
	return p.query(ctx, n.req)
}
