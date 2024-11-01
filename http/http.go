package http

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/nasdf/capy"
	"github.com/nasdf/capy/graphql"

	"github.com/99designs/gqlgen/graphql/playground"
)

// ListenAndServe starts an http server bound to the given address.
func ListenAndServe(db *capy.DB, addr string) error {
	http.Handle("/", playground.Handler("Capy", "/query"))
	http.Handle("/query", Handler(db))
	return http.ListenAndServe(addr, nil)
}

// Handler returns an http.Handler that can serve GraphQL requests.
func Handler(db *capy.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var params graphql.QueryParams
		switch r.Method {
		case http.MethodGet:
			query := r.URL.Query()
			params.Query = query.Get("query")
			params.OperationName = query.Get("operationName")
			if !query.Has("variables") {
				break
			}
			err := json.Unmarshal([]byte(query.Get("variables")), &params.Variables)
			if err != nil {
				http.Error(w, fmt.Sprintf("failed to parse variables: %v", err), http.StatusBadRequest)
				return
			}

		case http.MethodPost:
			err := json.NewDecoder(r.Body).Decode(&params)
			if err != nil {
				http.Error(w, fmt.Sprintf("failed to parse body: %v", err), http.StatusBadRequest)
				return
			}

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		resp := graphql.QueryResponse{}
		data, err := db.Execute(r.Context(), params)
		if err != nil {
			resp.Errors = append(resp.Errors, err.Error())
		}
		resp.Data = data
		out, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(out)
	})
}
