//go:build !js

package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/nasdf/capy"
	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/link"
	"github.com/nasdf/capy/storage"

	"github.com/99designs/gqlgen/graphql/playground"
	jsonc "github.com/ipld/go-ipld-prime/codec/json"
)

//go:embed schema.graphql
var schema string

func main() {
	ctx := context.Background()

	address := "localhost:8080"
	if len(os.Args) >= 2 {
		address = os.Args[1]
	}

	links := link.NewStore(storage.NewMemory())
	db, err := capy.Open(ctx, links, schema)
	if err != nil {
		panic(err)
	}
	http.Handle("/", playground.Handler("Capy", "/query"))
	http.Handle("/query", handler(db))

	fmt.Printf("Open a browser and navigate to %s\n", address)
	err = http.ListenAndServe(address, nil)
	if err != nil {
		panic(err)
	}
}

// handler returns an http.handler that can serve GraphQL requests.
func handler(db *core.Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var params graphql.QueryParams
		var err error
		switch r.Method {
		case http.MethodGet:
			values := r.URL.Query()
			params.Query = values.Get("query")
			params.OperationName = values.Get("operationName")
			if values.Has("variables") {
				err = json.Unmarshal([]byte(values.Get("variables")), &params.Variables)
			}
		case http.MethodPost:
			err = json.NewDecoder(r.Body).Decode(&params)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to parse request: %v", err), http.StatusBadRequest)
			return
		}
		res, err := capy.Execute(r.Context(), db, params)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		err = jsonc.Encode(res, w)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}
