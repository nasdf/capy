package capy

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"text/template"

	"github.com/nasdf/capy/core"
	"github.com/nasdf/capy/graphql"
	"github.com/nasdf/capy/storage"
	"github.com/nasdf/capy/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapy(t *testing.T) {
	paths, err := test.TestCasePaths()
	require.NoError(t, err, "failed to walk test cases dir")

	for _, path := range paths {
		testCase, err := test.LoadTestCase(path)
		require.NoError(t, err, "failed to load test case %s", path)

		t.Run(path, func(st *testing.T) {
			st.Parallel()

			ctx := context.Background()
			store := core.Open(storage.NewMemory())

			db, err := New(ctx, store, testCase.Schema)
			require.NoError(st, err, "failed to create db")

			for _, op := range testCase.Operations {
				data, err := db.Execute(ctx, op.Params)
				require.NoError(st, err)

				actual, err := json.Marshal(graphql.QueryResponse{Data: data, Errors: err})
				require.NoError(st, err)

				expected, err := json.Marshal(op.Response)
				require.NoError(st, err)

				tpl, err := template.New("response").Parse(string(expected))
				require.NoError(st, err)

				var out bytes.Buffer
				err = tpl.Execute(&out, data)
				require.NoError(st, err)

				assert.JSONEq(st, out.String(), string(actual))
			}
		})
	}
}
