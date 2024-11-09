package tests

import (
	"context"
	"embed"
	"io/fs"
	"testing"

	"github.com/nasdf/capy"
	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/graphql"

	"github.com/BurntSushi/toml"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed cases/*
var testCaseFS embed.FS

type TestCaseFile struct {
	// Cases is a list of all cases to test within a single file.
	Cases []TestCase `toml:"cases"`
}

type TestCase struct {
	// Name is a simple description for the test case.
	Name string `toml:"name"`
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

func (tc TestCase) Run(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := core.Open(ctx, &memstore.Store{})

	db, err := capy.New(ctx, store, tc.Schema)
	require.NoError(t, err, "failed to create db")

	for _, op := range tc.Operations {
		data, err := db.Execute(ctx, op.Params)
		require.NoError(t, err)

		assert.Equal(t, op.Response.Data, data)
		assert.Equal(t, op.Response.Errors, err)
	}
}

func TestAllCases(t *testing.T) {
	paths, err := fs.Glob(testCaseFS, "cases/*.toml")
	require.NoError(t, err)

	for _, path := range paths {
		t.Logf("Reading test cases: %s", path)
		data, err := fs.ReadFile(testCaseFS, path)
		require.NoError(t, err, "failed to read file: %s", path)

		var test TestCaseFile
		err = toml.Unmarshal(data, &test)
		require.NoError(t, err, "failed to read file: %s", path)

		for _, tc := range test.Cases {
			t.Run(tc.Name, tc.Run)
		}
	}
}
