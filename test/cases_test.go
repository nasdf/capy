package test

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"path/filepath"
	"testing"
	"text/template"

	"github.com/nasdf/capy"
	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/graphql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

//go:embed cases
var casesFS embed.FS

type TestCase struct {
	// Schema is the GraphQL Schema used to create a Capy instance.
	Schema string
	// Operations is a list of all GraphQL Operations to run in this test case.
	Operations []Operation
}

func (tc TestCase) Run(t *testing.T) {
	ctx := context.Background()

	db, err := capy.Init(ctx, core.NewMemoryStorage(), tc.Schema)
	require.NoError(t, err, "failed to create db")

	for _, op := range tc.Operations {
		docs, err := db.Dump(ctx)
		require.NoError(t, err, "failed to load documents")

		query, err := op.QueryTemplate(ctx, docs)
		require.NoError(t, err, "failed to execute query template")

		result := graphql.Execute(ctx, db, graphql.QueryParams{Query: query})
		actual, err := json.Marshal(result)
		require.NoError(t, err, "failed to encode results")

		expected, err := op.ResponseTemplate(ctx, docs)
		require.NoError(t, err, "failed to execute response template")

		assert.JSONEq(t, expected, string(actual))
	}
}

type Operation struct {
	// Query contains the Query document for this operation.
	Query string
	// Response contains the expected GraphQL Response.
	Response string
}

func (o Operation) QueryTemplate(ctx context.Context, rootValue any) (string, error) {
	tpl, err := template.New("response").Parse(o.Query)
	if err != nil {
		return "", err
	}
	var data bytes.Buffer
	if err := tpl.Execute(&data, rootValue); err != nil {
		return "", err
	}
	return data.String(), nil
}

func (o Operation) ResponseTemplate(ctx context.Context, rootValue any) (string, error) {
	tpl, err := template.New("response").Parse(o.Response)
	if err != nil {
		return "", err
	}
	var data bytes.Buffer
	if err := tpl.Execute(&data, rootValue); err != nil {
		return "", err
	}
	return data.String(), nil
}

func TestAllCases(t *testing.T) {
	fs.WalkDir(casesFS, "cases", func(path string, d fs.DirEntry, err error) error {
		if filepath.Ext(path) != ".yaml" && err == nil {
			return nil
		}
		require.NoError(t, err, "failed to walk cases directory")

		data, err := fs.ReadFile(casesFS, path)
		require.NoError(t, err, "failed to read test case file")

		var testCase TestCase
		err = yaml.Unmarshal(data, &testCase)
		require.NoError(t, err, "failed to parse test case file")

		t.Run(path, testCase.Run)
		return nil
	})
}
