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

package disk

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func TestBinPersister(t *testing.T) {
	t.Run("add keeps track of vectors in bin", func(t *testing.T) {
		binPersister := newFloatBinPersister(t.TempDir(), 1536)
		v, _ := testinghelpers.RandomVecs(2, 0, 1536)
		binPersister.addBin(0, map[uint64][]float32{13: v[0]})

		vectors := binPersister.getBin(0)
		assert.Equal(t, int(1), len(vectors))
		vec1 := vectors[13]
		assert.ElementsMatch(t, v[0], vec1)

		binPersister.addVectorToBin(0, 17, v[1])
		vectors = binPersister.getBin(0)
		assert.Equal(t, int(2), len(vectors))
		vec1 = vectors[13]
		vec2 := vectors[17]
		assert.ElementsMatch(t, v[0], vec1)
		assert.ElementsMatch(t, v[1], vec2)
	})
}
