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
	"math"
	"os"
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/disk"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type memoryManager[T float32 | byte | uint64] struct {
	idsInUse   []bool
	binUsed    []int
	binsUsedBy []map[int]interface{}
}

type diskCache[T float32 | byte | uint64] struct {
	sync.Mutex
	cache         cache.Cache[T]
	memoryManager *memoryManager[T]
	binManager    *disk.BinManager[T]
	shardedLocks  *common.ShardedRWLocks
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
	total := 0
	dc.memoryManager.idsInUse[id] = false
	for i := range dc.memoryManager.binsUsedBy[id] {
		size := dc.binManager.GetBinSize(i)
		total += size
		dc.memoryManager.binUsed[i]--
		if dc.memoryManager.binUsed[i] == 0 {
			dc.dropBin(i)
		}
	}
	dc.memoryManager.binsUsedBy[id] = make(map[int]interface{})
	dc.Unlock()
}

func NewDiskCacheFloat(cache cache.Cache[float32]) cache.Cache[float32] {
	chunksLoaded := make([]bool, 100_000)
	chunksLoaded[0] = true
	binsUsedBy := make([]map[int]interface{}, 20)
	for i := range binsUsedBy {
		binsUsedBy[i] = make(map[int]interface{})
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
		binManager:   disk.NewFloatBinManager(100, distancer.NewCosineDistanceProvider(), shardedLocks),
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
	dc.shardedLocks.RUnlock(uint64(binId))
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
	_, ok := binsUsed[binId]
	if !ok {
		binsUsed[binId] = true
		dc.memoryManager.binUsed[binId]++
		usedBefore := dc.memoryManager.binUsed[binId] > 1
		dc.Unlock()
		if !usedBefore {
			dc.loadBin(binId)
		}
	} else {
		dc.Unlock()
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
	dc.shardedLocks.Lock(uint64(id))
	defer dc.shardedLocks.Unlock(uint64(id))
	vecs := make([][]float32, 100)
	conns := make([][]uint32, 100)
	fi, err := os.Open("output.txt")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()
	buf := make([]byte, (1532+16)*4)
	_, err = fi.Read(buf)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 100; i++ {
		vecs[i] = make([]float32, 1532)
		conns[i] = make([]uint32, 16)
		for j := 0; j < 1532; j++ {
			bits := binary.LittleEndian.Uint32(buf)
			vecs[i][j] = math.Float32frombits(bits)
		}
		for j := 0; j < 16; j++ {
			conns[i][j] = binary.LittleEndian.Uint32(buf)
		}
	}
}

func (dc *diskCache[T]) dropBin(id int) {
}
