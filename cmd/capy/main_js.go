package main

import (
	"context"
	"fmt"
	"syscall/js"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/jsutil"
)

// tinygo build -o capy.wasm -no-debug -scheduler=none ./cmd/capy

func main() {
	fmt.Printf("Capy initialized...")
}

//export init
func initRepository(storage js.Value, schema string) js.Value {
	return jsutil.NewPromise(func(resolve, reject func(args ...any) js.Value) any {
		_, err := core.InitRepository(context.Background(), core.NewJSStorage(storage), schema)
		if err != nil {
			return reject(jsutil.NewError(err))
		}
		return resolve(js.Undefined())
	})
}

//export execute
func execute(storage js.Value, query string, operationName string, variables map[string]any) js.Value {
	return jsutil.NewPromise(func(resolve, reject func(args ...any) js.Value) any {
		repo, err := core.OpenRepository(context.Background(), core.NewJSStorage(storage))
		if err != nil {
			return reject(jsutil.NewError(err))
		}
		params := graphql.QueryParams{
			Query:         query,
			OperationName: operationName,
			Variables:     variables,
		}
		result := graphql.Execute(context.Background(), repo, params)
		return resolve(js.ValueOf(result.ToMap()))
	})
}
