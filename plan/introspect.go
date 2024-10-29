package plan

import "context"

type introspectNode struct {
	res map[string]any
}

func Introspect(res map[string]any) Node {
	return &introspectNode{
		res: res,
	}
}

func (n *introspectNode) Execute(ctx context.Context, p *Planner) (any, error) {
	return n.res, nil
}
