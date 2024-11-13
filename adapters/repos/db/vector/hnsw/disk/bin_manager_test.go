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

package disk_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/disk"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func TestBinManager(t *testing.T) {
	t.Run("add keeps track of vectors in bin", func(t *testing.T) {
		bm := disk.NewFloatBinManager(100, distancer.NewL2SquaredProvider())
		assert.Nil(t, bm.Add(-1, 0, make([]float32, 4)))
		bin, err := bm.GetBinOfVector(0)
		assert.Nil(t, err)
		assert.Equal(t, int(0), bin)

		bin, err = bm.GetBinOfVector(0)
		assert.Nil(t, err)
		assert.Nil(t, bm.Add(0, 1, make([]float32, 4)))
		assert.Equal(t, int(0), bin)

	})

	t.Run("error when wrong closestId", func(t *testing.T) {
		bm := disk.NewFloatBinManager(100, distancer.NewL2SquaredProvider())
		assert.Nil(t, bm.Add(-1, 0, make([]float32, 4)))
		bin, err := bm.GetBinOfVector(0)
		assert.Nil(t, err)
		assert.Equal(t, int(0), bin)

		assert.NotNil(t, bm.Add(5, 1, make([]float32, 4)))
	})

	t.Run("split when max is reached", func(t *testing.T) {
		vecs, _ := testinghelpers.RandomVecs(11, 0, 4)
		bm := disk.NewFloatBinManager(10, distancer.NewL2SquaredProvider())
		for i := uint64(0); i < 11; i++ {
			assert.Nil(t, bm.Add(0, i, vecs[i]))
		}
		bin0Exists := false
		bin1Exists := false
		for i := uint64(0); i < 11; i++ {
			bin, err := bm.GetBinOfVector(i)
			assert.Nil(t, err)
			if bin == 0 {
				bin0Exists = true
			} else {
				bin1Exists = true
			}
		}
		assert.True(t, bin0Exists)
		assert.True(t, bin1Exists)
	})
}
