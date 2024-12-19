//go:build js

package core_js

import (
	"context"
	"syscall/js"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/graphql"
)

func InitRepository(storage js.Value, schema string) js.Value {
	return NewPromise(func(resolve, reject func(value js.Value) js.Value) any {
		_, err := core.InitRepository(context.Background(), NewStorage(storage), schema)
		if err != nil {
			return reject(NewError(err))
		}
		return resolve(js.Undefined())
	})
}

func Execute(storage js.Value, query string, operationName string, variables map[string]any) js.Value {
	return NewPromise(func(resolve, reject func(value js.Value) js.Value) any {
		repo, err := core.OpenRepository(context.Background(), NewStorage(storage))
		if err != nil {
			return reject(NewError(err))
		}
		params := graphql.QueryParams{
			Query:         query,
			OperationName: operationName,
			Variables:     variables,
		}
		result := graphql.Execute(context.Background(), repo, params)
		return resolve(js.ValueOf(result))
	})
}
