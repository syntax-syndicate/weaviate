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
	"fmt"
	"math/rand"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

type BinManager[T float32 | byte | uint64] struct {
	binMaxSize   int
	bins         []int
	vecs         [][]uint64
	binPersister *binPersister[T]
	maxBin       int
	distancer    compressionhelpers.GenericDistancer[T]
}

func NewFloatBinManager(binMaxSize int, distancer compressionhelpers.GenericDistancer[float32]) *BinManager[float32] {
	return &BinManager[float32]{
		binMaxSize:   binMaxSize,
		binPersister: newFloatBinPersister(),
		distancer:    distancer,
	}
}

func NewByteBinManager(binMaxSize int, distancer compressionhelpers.GenericDistancer[byte]) *BinManager[byte] {
	return &BinManager[byte]{
		binMaxSize:   binMaxSize,
		binPersister: newByteBinPersister(),
		distancer:    distancer,
	}
}

func NewUintBinManager(binMaxSize int, distancer compressionhelpers.GenericDistancer[uint64]) *BinManager[uint64] {
	return &BinManager[uint64]{
		binMaxSize:   binMaxSize,
		binPersister: newUintBinPersister(),
		distancer:    distancer,
	}
}

func (bm *BinManager[T]) Add(closestId int, vecId uint64, vector []T) error {
	if len(bm.bins) == 0 {
		bm.bins = make([]int, vecId+1)
		bm.bins[vecId] = 0
		bm.binPersister.addVectorToBin(0, vecId, vector)
		bm.vecs = append(bm.vecs, []uint64{0})
		return nil
	}
	if closestId > len(bm.bins) {
		return fmt.Errorf("unknown bin id: %d, while trying to add a vector to the bins", closestId)
	}
	binId := bm.bins[closestId]
	if len(bm.bins) <= int(vecId) {
		oldBins := bm.bins
		bm.bins = make([]int, vecId+1+vecId/10)
		copy(bm.bins, oldBins)
	}
	if len(bm.vecs[binId]) == bm.binMaxSize {
		bin := bm.binPersister.getBin(binId)
		bin[vecId] = vector
		bm.bins[vecId] = binId
		bin1, bin2, err := bm.splitBin(bin)
		if err != nil {
			return nil
		}
		bm.binPersister.updateBin(binId, bin1)
		bm.maxBin++
		bm.binPersister.addBin(bm.maxBin, bin2)
		bm.vecs[binId] = Keys(bin1)
		bm.vecs = append(bm.vecs, Keys(bin2))
		for key := range bin2 {
			bm.bins[key] = bm.maxBin
		}

		return nil
	}
	bm.binPersister.addVectorToBin(binId, vecId, vector)
	bm.bins[vecId] = binId
	bm.vecs[binId] = append(bm.vecs[binId], vecId)
	return nil
}

func Keys[M ~map[K]V, K comparable, V any](m M) []K {
	r := make([]K, 0, len(m))
	for k := range m {
		r = append(r, k)
	}
	return r
}

func (bm *BinManager[T]) GetBinOfVector(vecId uint64) (int, error) {
	if len(bm.bins) >= int(vecId) {
		return bm.bins[vecId], nil
	}
	return -1, fmt.Errorf("unknown id: %d, while trying to get the bin of a vector", vecId)
}

func (bm *BinManager[T]) splitBin(vectors map[uint64][]T) (map[uint64][]T, map[uint64][]T, error) {
	centroid1 := rand.Uint64() % uint64(len(vectors))
	centroid2 := rand.Uint64() % uint64(len(vectors))
	for centroid2 == centroid1 {
		centroid2 = rand.Uint64() % uint64(len(vectors))
	}
	bin1 := make(map[uint64][]T)
	bin2 := make(map[uint64][]T)
	for key, val := range vectors {
		d1, err := bm.distancer.SingleDist(vectors[centroid1], val)
		if err != nil {
			return nil, nil, err
		}
		d2, err := bm.distancer.SingleDist(vectors[centroid2], val)
		if err != nil {
			return nil, nil, err
		}
		if d1 < d2 {
			bin1[key] = val
		} else {
			bin2[key] = val
		}
	}
	return bin1, bin2, nil
}
