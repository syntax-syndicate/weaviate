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
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/disk"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type memoryManager[T float32 | byte | uint64] struct {
	idsInUse   []bool
	binUsed    []int
	binsUsedBy []map[int]bool
}

type diskCache[T float32 | byte | uint64] struct {
	sync.Mutex
	cache         cache.Cache[T]
	memoryManager *memoryManager[T]
	binManager    *disk.BinManager[T]
	shardedLocks  *common.ShardedRWLocks
	raw           [][]byte
}

func (dc *diskCache[T]) GenerateCallerId() int {
	dc.Lock()
	defer dc.Unlock()
	for i, loaded := range dc.memoryManager.idsInUse {
		if !loaded {
			dc.memoryManager.idsInUse[i] = true
			return i
		}
	}
	return 0
}

func (dc *diskCache[T]) ReturnCallerId(id int) {
	dc.Lock()
	dc.memoryManager.idsInUse[id] = false
	for i := range dc.memoryManager.binsUsedBy[id] {
		dc.memoryManager.binUsed[i]--
		if dc.memoryManager.binUsed[i] == 0 {
			fmt.Println("dropping ", i)
			dc.dropBin(i)
		}
	}
	dc.memoryManager.binsUsedBy[id] = make(map[int]bool)
	dc.Unlock()
}

func NewDiskCacheFloat(path string, cache cache.Cache[float32]) cache.Cache[float32] {
	chunksLoaded := make([]bool, 100_000)
	chunksLoaded[0] = true
	binsUsedBy := make([]map[int]bool, 20)
	for i := range binsUsedBy {
		binsUsedBy[i] = make(map[int]bool)
	}

	shardedLocks := common.NewDefaultShardedRWLocks()
	return &diskCache[float32]{
		cache: cache,
		memoryManager: &memoryManager[float32]{
			idsInUse:   chunksLoaded,
			binUsed:    make([]int, 100_000),
			binsUsedBy: binsUsedBy,
		},
		shardedLocks: shardedLocks,
		binManager:   disk.NewFloatBinManager(path, 100, distancer.NewCosineDistanceProvider(), shardedLocks),
		raw:          make([][]byte, 100_000),
	}
}

func NewDiskCacheByte(cache cache.Cache[byte]) cache.Cache[byte] {
	return &diskCache[byte]{
		cache:         cache,
		memoryManager: &memoryManager[byte]{},
		shardedLocks:  common.NewDefaultShardedRWLocks(),
	}
}

func NewDiskCacheUint64(cache cache.Cache[uint64]) cache.Cache[uint64] {
	return &diskCache[uint64]{
		cache:         cache,
		memoryManager: &memoryManager[uint64]{},
		shardedLocks:  common.NewDefaultShardedRWLocks(),
	}
}

func (dc *diskCache[T]) Get(ctx context.Context, id uint64, callerId int) ([]T, error) {
	binId, _ := dc.binManager.GetBinOfVector(id)
	dc.shardedLocks.RLock(uint64(binId))
	if binId > -1 && binId < len(dc.raw) && dc.raw[binId] != nil {
		valBuf := make([]T, 1536)
		binary.LittleEndian.Uint64(dc.raw[binId])
		for j := 0; j < 1536; j++ {
			valBuf[j] = T(math.Float32frombits(binary.LittleEndian.Uint32(dc.raw[binId][8+j*4:])))
		}
	}
	defer dc.shardedLocks.RUnlock(uint64(binId))
	return dc.cache.Get(ctx, id, callerId)
}

func (dc *diskCache[T]) MultiGet(ctx context.Context, ids []uint64, callerId int) ([][]T, []error) {
	return dc.cache.MultiGet(ctx, ids, callerId)
}

func (dc *diskCache[T]) Len() int32 {
	return dc.cache.Len()
}

func (dc *diskCache[T]) CountVectors() int64 {
	return dc.cache.CountVectors()
}

func (dc *diskCache[T]) Delete(ctx context.Context, id uint64) {
	dc.cache.Delete(ctx, id)
}

func (dc *diskCache[T]) DeleteNoLock(ctx context.Context, id uint64) {
	dc.cache.DeleteNoLock(ctx, id)
}

func (dc *diskCache[T]) Connect(id, closestId uint64, vec []T) {
	dc.binManager.Add(closestId, id, vec)
}

func (dc *diskCache[T]) Preload(id uint64, vec []T) {
	dc.cache.Preload(id, vec)
}

func (dc *diskCache[T]) PreloadNoLock(id uint64, vec []T) {
	dc.cache.PreloadNoLock(id, vec)
}

func (dc *diskCache[T]) SetSizeAndGrowNoLock(id uint64) {
	dc.cache.SetSizeAndGrowNoLock(id)
}

func (dc *diskCache[T]) Prefetch(id uint64, callerId int) {
	dc.Lock()
	binId, _ := dc.binManager.GetBinOfVector(id)
	if binId == -1 {
		dc.Unlock()
		return
	}
	binsUsed := dc.memoryManager.binsUsedBy[callerId]
	dc.memoryManager.binUsed[binId]++
	_, ok := binsUsed[binId]
	dc.memoryManager.binsUsedBy[callerId][binId] = true
	usedBefore := dc.memoryManager.binUsed[binId] > 1
	dc.Unlock()
	if !ok {
		if !usedBefore {
			dc.shardedLocks.Lock(uint64(binId))
			go func() {
				dc.loadBin(binId)
			}()
		}
	}
	dc.cache.Prefetch(id, callerId)
}

func (dc *diskCache[T]) Grow(size uint64) {
	dc.cache.Grow(size)
}

func (dc *diskCache[T]) Drop() {
	dc.cache.Drop()
}

func (dc *diskCache[T]) UpdateMaxSize(size int64) {
	dc.cache.UpdateMaxSize(size)
}

func (dc *diskCache[T]) CopyMaxSize() int64 {
	return dc.cache.CopyMaxSize()
}

func (dc *diskCache[T]) All() [][]T {
	return dc.cache.All()
}

func (dc *diskCache[T]) LockAll() {
	dc.cache.LockAll()
}

func (dc *diskCache[T]) UnlockAll() {
	dc.cache.UnlockAll()
}

func (dc *diskCache[T]) loadBin(id int) {
	dc.raw[id] = dc.binManager.GetRawBin(id)
	defer dc.shardedLocks.Unlock(uint64(id))
}

func (dc *diskCache[T]) dropBin(id int) {
	dc.shardedLocks.Lock(uint64(id))
	defer dc.shardedLocks.Unlock(uint64(id))
	for _, i := range dc.binManager.GetVecIdsInBin(id) {
		dc.cache.DeleteNoLock(context.Background(), i)
	}
}
