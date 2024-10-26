package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpawnTypeSystem(t *testing.T) {
	const schema = `type KitchenSink {
		int: Int
		intList: [Int]
		nonNullIntList: [Int!]

		float: Float
		floatList: [Float]
		nonNullFloatList: [Float!]

		string: String
		stringList: [String]
		nonNullStringList: [String!]

		boolean: Boolean
		booleanList: [Boolean]
		nonNullBooleanList: [Boolean!]

		ref: KitchenSink
		refList: [KitchenSink]
		nonNullRefList: [KitchenSink!]
	}`

	sys, err := SpawnTypeSystem(schema)
	require.NoError(t, err)

	rootType := sys.TypeByName(RootTypeName)
	require.NotNil(t, rootType)

	assert.Equal(t, RootTypeName, rootType.Name())
}
