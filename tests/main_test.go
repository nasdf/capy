package tests

import (
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/nasdf/capy"
	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/graphql"

	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// paths is a list of all test case paths
var paths []string

func init() {
	// get a relative path to all test case files
	err := fs.WalkDir(os.DirFS("."), "cases", func(path string, d fs.DirEntry, err error) error {
		if filepath.Ext(path) == ".yaml" {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

type TestCase struct {
	// Description is a simple description for the test case.
	Description string `toml:"description"`
	// Schema is the GraphQL schema used to create a Capy instance.
	Schema string `toml:"schema"`
	// Operations is a list of all GraphQL operations to run in this test case.
	Operations []TestCaseOperation `toml:"operations"`
}

type TestCaseOperation struct {
	// Params contains the GraphQL parameters for this operation.
	Params graphql.QueryParams `toml:"params"`
	// Response contains the expected GraphQL response.
	Response graphql.QueryResponse `toml:"response"`
}

func TestAllCases(t *testing.T) {
	for _, path := range paths {
		t.Logf("Running test cases: %s", path)
		data, err := os.ReadFile(path)
		require.NoError(t, err, "failed to read file: %s", path)

		var testCase TestCase
		err = yaml.Unmarshal(data, &testCase)
		require.NoError(t, err, "failed to read file: %s", path)

		t.Run(testCase.Description, func(st *testing.T) {
			st.Parallel()

			ctx := context.Background()
			store := core.Open(ctx, &memstore.Store{})

			db, err := capy.New(ctx, store, testCase.Schema)
			require.NoError(st, err, "failed to create db")

			for _, op := range testCase.Operations {
				data, err := db.Execute(ctx, op.Params)
				require.NoError(st, err)

				actual, err := json.Marshal(graphql.QueryResponse{Data: data, Errors: err})
				require.NoError(t, err)

				expected, err := json.Marshal(op.Response)
				require.NoError(t, err)

				assert.JSONEq(t, string(expected), string(actual))
			}
		})
	}
}
