package test

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
	"github.com/nasdf/capy/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type TestCase struct {
	// Description is a simple description for the test case.
	Description string
	// Schema is the GraphQL schema used to create a Capy instance.
	Schema string
	// Operations is a list of all GraphQL operations to run in this test case.
	Operations []TestCaseOperation
}

type TestCaseOperation struct {
	// Params contains the GraphQL parameters for this operation.
	Params graphql.QueryParams
	// Response contains the expected GraphQL response.
	Response graphql.QueryResponse
}

func (tc TestCase) Run(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := core.Open(storage.NewMemory())

	db, err := capy.New(ctx, store, tc.Schema)
	require.NoError(t, err, "failed to create db")

	for _, op := range tc.Operations {
		data, err := db.Execute(ctx, op.Params)
		require.NoError(t, err)

		actual, err := json.Marshal(graphql.QueryResponse{Data: data, Errors: err})
		require.NoError(t, err)

		expected, err := json.Marshal(op.Response)
		require.NoError(t, err)

		assert.JSONEq(t, string(expected), string(actual))
	}
}

func TestCases(t *testing.T) {
	var paths []string
	err := fs.WalkDir(os.DirFS("."), "cases", func(path string, d fs.DirEntry, err error) error {
		if filepath.Ext(path) == ".yaml" {
			paths = append(paths, path)
		}
		return err
	})
	require.NoError(t, err, "failed to walk test cases dir")

	for _, path := range paths {
		data, err := os.ReadFile(path)
		require.NoError(t, err, "failed to read test case file: %s", path)

		var testCase TestCase
		err = yaml.Unmarshal(data, &testCase)
		require.NoError(t, err, "failed to parse test case file: %s", path)

		t.Logf("Running test cases: %s", path)
		t.Run(testCase.Description, testCase.Run)
	}
}
