package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type Executor interface {
	Execute(context.Context, QueryParams) (any, error)
}

// Handler returns an http.Handler that can serve GraphQL requests.
func Handler(e Executor) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var params QueryParams
		var err error
		switch r.Method {
		case http.MethodGet:
			params, err = ParseGetQueryParams(r)
			if err != nil {
				http.Error(w, fmt.Sprintf("failed to parse request: %v", err), http.StatusBadRequest)
				return
			}

		case http.MethodPost:
			params, err = ParsePostQueryParams(r)
			if err != nil {
				http.Error(w, fmt.Sprintf("failed to parse request: %v", err), http.StatusBadRequest)
				return
			}

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		data, err := e.Execute(r.Context(), params)
		res := &QueryResponse{Data: data, Errors: err}
		err = json.NewEncoder(w).Encode(res)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func ParseGetQueryParams(r *http.Request) (QueryParams, error) {
	params := QueryParams{}
	values := r.URL.Query()
	params.Query = values.Get("query")
	params.OperationName = values.Get("operationName")
	if !values.Has("variables") {
		return params, nil
	}
	err := json.Unmarshal([]byte(values.Get("variables")), &params.Variables)
	if err != nil {
		return params, err
	}
	return params, nil
}

func ParsePostQueryParams(r *http.Request) (QueryParams, error) {
	params := QueryParams{}
	err := json.NewDecoder(r.Body).Decode(&params)
	if err != nil {
		return params, err
	}
	return params, nil
}
