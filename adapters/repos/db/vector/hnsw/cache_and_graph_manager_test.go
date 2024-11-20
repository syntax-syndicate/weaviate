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

package hnsw_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestIndexOnDisk(t *testing.T) {
	efConstruction := 16
	ef := 32
	maxNeighbors := 8
	dimensions := 1536
	vectors_size := 1000
	queries_size := 10
	switch_at := vectors_size
	before := time.Now()
	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
	distancer := distancer.NewCosineDistanceProvider()
	fmt.Printf("generating data took %s\n", time.Since(before))

	uc := ent.UserConfig{}
	uc.MaxConnections = maxNeighbors
	uc.EFConstruction = efConstruction
	uc.EF = ef
	uc.VectorCacheMaxObjects = 10e12

	index, _ := hnsw.New(hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "recallbenchmark",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      distancer,
		VectorForIDThunk: func(ctx context.Context, id uint64, callerId int) ([]float32, error) {
			return vectors[int(id)], nil
		},
	}, uc, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	logger := logrus.New()
	compressionhelpers.Concurrently(logger, uint64(switch_at), func(id uint64) {
		assert.Nil(t, index.Add(context.Background(), uint64(id), vectors[id]))
	})
	fmt.Println(time.Since(before))
	compressionhelpers.Concurrently(logger, uint64(queries_size), func(id uint64) {
		v, _, e := index.SearchByVector(context.Background(), queries[id], 10, nil)
		assert.Nil(t, e)
		assert.Equal(t, 10, len(v))
	})
	fmt.Println(time.Since(before))
}
