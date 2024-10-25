package plan

import (
	"context"
	"fmt"
	"strings"
)

type mutationNode struct {
	req Request
}

func Mutation(req Request) Node {
	return &mutationNode{
		req: req,
	}
}

func (n *mutationNode) Execute(ctx context.Context, p *Planner) (*Result, error) {
	for i, f := range n.req.Fields {
		if !strings.HasPrefix(f.Name, "create") {
			return nil, fmt.Errorf("unsupported operation %s", f.Name)
		}
		collection := strings.TrimPrefix(f.Name, "create")
		lnk, err := p.create(ctx, collection, f.Arguments["input"])
		if err != nil {
			return nil, err
		}
		n.req.Fields[i].Name = collection
		n.req.Fields[i].Arguments["id"] = lnk
	}
	return Query(n.req).Execute(ctx, p)
}
