//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
)

func TestSearchByDistParams(t *testing.T) {
	t.Run("param iteration", func(t *testing.T) {
		params := newSearchByDistParams(100)
		assert.Equal(t, 0, params.offset)
		assert.Equal(t, DefaultSearchByDistInitialLimit, params.limit)
		assert.Equal(t, 100, params.totalLimit)

		params.iterate()
		assert.Equal(t, 100, params.offset)
		assert.Equal(t, 1000, params.limit)
		assert.Equal(t, 1100, params.totalLimit)
	})
}

func TestFastAllowList(t *testing.T) {
	visited := visited.NewSparseSet(1_000_000, 8192)
	visited.Visit(100)
	visited.Visit(1010)
	visited.Visit(10)
	assert.True(t, visited.Visited(100))
	assert.True(t, visited.Visited(1010))
	assert.True(t, visited.Visited(10))
	assert.False(t, visited.Visited(101))
	assert.False(t, visited.Visited(99))
	assert.False(t, visited.Visited(500000))
	visited.Reset()
	assert.False(t, visited.Visited(100))

	allow := helpers.NewAllowList(100, 1010, 10)
	sparseAllow := NewFastSet(allow, visited)
	assert.True(t, sparseAllow.Contains(100))
	assert.True(t, sparseAllow.Contains(1010))
	assert.True(t, sparseAllow.Contains(10))
	assert.False(t, sparseAllow.Contains(101))
	assert.False(t, sparseAllow.Contains(99))
	assert.False(t, sparseAllow.Contains(500000))

	it := sparseAllow.Iterator()
	for id, ok := it.Next(); ok; id, ok = it.Next() {
		assert.True(t, id == uint64(100) || id == uint64(1010) || id == uint64(10))
	}
}
