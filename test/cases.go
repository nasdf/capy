package test

import (
	"embed"
	"io/fs"
	"path/filepath"

	"github.com/nasdf/capy/graphql"
	"gopkg.in/yaml.v3"
)

//go:embed cases
var casesFS embed.FS

type TestCase struct {
	// Schema is the GraphQL schema used to create a Capy instance.
	Schema string
	// Operations is a list of all GraphQL operations to run in this test case.
	Operations []Operation
}

type Operation struct {
	// Params contains the GraphQL parameters for this operation.
	Params graphql.QueryParams
	// Response contains the expected GraphQL response.
	Response string
}

// TestCasePaths returns a list of all test case file paths.
func TestCasePaths() (paths []string, _ error) {
	return paths, fs.WalkDir(casesFS, "cases", func(path string, d fs.DirEntry, err error) error {
		if filepath.Ext(path) == ".yaml" {
			paths = append(paths, path)
		}
		return err
	})
}

// LoadTestCase loads and parses a test case file.
func LoadTestCase(path string) (*TestCase, error) {
	data, err := fs.ReadFile(casesFS, path)
	if err != nil {
		return nil, err
	}
	var testCase TestCase
	if err := yaml.Unmarshal(data, &testCase); err != nil {
		return nil, err
	}
	return &testCase, nil
}
