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

type binPersister[T float32 | byte | uint64] struct {
	vectors []map[uint64][]T
}

func newFloatBinPersister() *binPersister[float32] {
	return &binPersister[float32]{
		vectors: []map[uint64][]float32{make(map[uint64][]float32)},
	}
}

func newByteBinPersister() *binPersister[byte] {
	return &binPersister[byte]{
		vectors: []map[uint64][]byte{make(map[uint64][]byte)},
	}
}

func newUintBinPersister() *binPersister[uint64] {
	return &binPersister[uint64]{
		vectors: []map[uint64][]uint64{make(map[uint64][]uint64)},
	}
}

func (bp *binPersister[T]) addVectorToBin(binId int, id uint64, vector []T) {
	bp.vectors[binId][id] = vector
}

func (bp *binPersister[T]) getBin(binId int) map[uint64][]T {
	return bp.vectors[binId]
}

func (bp *binPersister[T]) updateBin(binId int, vectors map[uint64][]T) {
	bp.vectors[binId] = vectors
}

func (bp *binPersister[T]) addBin(binId int, vectors map[uint64][]T) {
	if len(bp.vectors) <= binId {
		old := bp.vectors
		bp.vectors = make([]map[uint64][]T, binId+1)
		copy(bp.vectors, old)
	}
	bp.vectors[binId] = vectors
}
