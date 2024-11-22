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

package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/memwatch"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type shardedLockCache[T float32 | byte | uint64] struct {
	shardedLocks     *common.ShardedRWLocks
	cache            [][]T
	vectorForID      common.VectorForID[T]
	normalizeOnRead  bool
	maxSize          int64
	count            int64
	cancel           chan bool
	logger           logrus.FieldLogger
	deletionInterval time.Duration
	allocChecker     memwatch.AllocChecker

	// The maintenanceLock makes sure that only one maintenance operation, such
	// as growing the cache or clearing the cache happens at the same time.
	maintenanceLock sync.RWMutex
}

const (
	InitialSize             = 1000
	MinimumIndexGrowthDelta = 2000
	indexGrowthRate         = 1.25
)

func NewShardedFloat32LockCache(vecForID common.VectorForID[float32], maxSize int, pageSize uint64,
	logger logrus.FieldLogger, normalizeOnRead bool, deletionInterval time.Duration,
	allocChecker memwatch.AllocChecker,
) Cache[float32] {
	vc := &shardedLockCache[float32]{
		vectorForID: func(ctx context.Context, id uint64) ([]float32, error) {
			vec, err := vecForID(ctx, id)
			if err != nil {
				return nil, err
			}
			if normalizeOnRead {
				vec = distancer.Normalize(vec)
			}
			return vec, nil
		},
		cache:            make([][]float32, InitialSize),
		normalizeOnRead:  normalizeOnRead,
		count:            0,
		maxSize:          int64(maxSize),
		cancel:           make(chan bool),
		logger:           logger,
		shardedLocks:     common.NewShardedRWLocksWithPageSize(512, pageSize),
		maintenanceLock:  sync.RWMutex{},
		deletionInterval: deletionInterval,
		allocChecker:     allocChecker,
	}

	vc.watchForDeletion()
	return vc
}

func NewShardedByteLockCache(vecForID common.VectorForID[byte], maxSize int, pageSize uint64,
	logger logrus.FieldLogger, deletionInterval time.Duration,
	allocChecker memwatch.AllocChecker,
) Cache[byte] {
	vc := &shardedLockCache[byte]{
		vectorForID:      vecForID,
		cache:            make([][]byte, InitialSize),
		normalizeOnRead:  false,
		count:            0,
		maxSize:          int64(maxSize),
		cancel:           make(chan bool),
		logger:           logger,
		shardedLocks:     common.NewShardedRWLocksWithPageSize(512, pageSize),
		maintenanceLock:  sync.RWMutex{},
		deletionInterval: deletionInterval,
		allocChecker:     allocChecker,
	}

	vc.watchForDeletion()
	return vc
}

func NewShardedUInt64LockCache(vecForID common.VectorForID[uint64], maxSize int, pageSize uint64,
	logger logrus.FieldLogger, deletionInterval time.Duration,
	allocChecker memwatch.AllocChecker,
) Cache[uint64] {
	vc := &shardedLockCache[uint64]{
		vectorForID:      vecForID,
		cache:            make([][]uint64, InitialSize),
		normalizeOnRead:  false,
		count:            0,
		maxSize:          int64(maxSize),
		cancel:           make(chan bool),
		logger:           logger,
		shardedLocks:     common.NewShardedRWLocksWithPageSize(512, pageSize),
		maintenanceLock:  sync.RWMutex{},
		deletionInterval: deletionInterval,
		allocChecker:     allocChecker,
	}

	vc.watchForDeletion()
	return vc
}

func (s *shardedLockCache[T]) All() [][]T {
	return s.cache
}

func (s *shardedLockCache[T]) Get(ctx context.Context, id uint64) ([]T, error) {
	s.shardedLocks.RLock(id)
	vec := s.cache[id]
	s.shardedLocks.RUnlock(id)

	if vec != nil {
		return vec, nil
	}

	return s.handleCacheMiss(ctx, id, true)
}

func (s *shardedLockCache[T]) Delete(ctx context.Context, id uint64) {
	s.shardedLocks.Lock(id)
	defer s.shardedLocks.Unlock(id)

	if int(id) >= len(s.cache) || s.cache[id] == nil {
		return
	}

	s.cache[id] = nil
	atomic.AddInt64(&s.count, -1)
}

func (s *shardedLockCache[T]) handleCacheMiss(ctx context.Context, id uint64, lock bool) ([]T, error) {
	if s.allocChecker != nil {
		// we don't really know the exact size here, but we don't have to be
		// accurate. If mem pressure is this high, we basically want to prevent any
		// new permanent heap alloc. If we underestimate the size of a vector a
		// bit, we might allow one more vector than we can really fit. But the one
		// after would fail then. Given that vectors are typically somewhat small
		// (in the single-digit KBs), it doesn't matter much, as long as we stop
		// allowing vectors when we're out of memory.
		estimatedSize := int64(1024)
		if err := s.allocChecker.CheckAlloc(estimatedSize); err != nil {
			s.logger.WithFields(logrus.Fields{
				"action": "vector_cache_miss",
				"event":  "vector_load_skipped_oom",
				"doc_id": id,
			}).WithError(err).
				Warnf("cannot load vector into cache due to memory pressure")
			return nil, err
		}
	}

	vec, err := s.vectorForID(ctx, id)
	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&s.count, 1)
	if lock {
		s.shardedLocks.Lock(id)
		s.shardedLocks.Unlock(id)
	}
	s.cache[id] = vec

	return vec, nil
}

func (s *shardedLockCache[T]) MultiGet(ctx context.Context, ids []uint64) ([][]T, []error) {
	out := make([][]T, len(ids))
	errs := make([]error, len(ids))

	for i, id := range ids {
		s.shardedLocks.RLock(id)
		vec := s.cache[id]
		s.shardedLocks.RUnlock(id)

		if vec == nil {
			vecFromDisk, err := s.handleCacheMiss(ctx, id, true)
			errs[i] = err
			vec = vecFromDisk
		}

		out[i] = vec
	}

	return out, errs
}

func (s *shardedLockCache[T]) GetAllInCurrentLock(ctx context.Context, id uint64, out [][]T, errs []error) ([][]T, []error, uint64, uint64) {
	start := (id / s.shardedLocks.PageSize) * s.shardedLocks.PageSize
	end := start + s.shardedLocks.PageSize

	if end > uint64(len(s.cache)) {
		end = uint64(len(s.cache))
	}

	s.shardedLocks.RLock(start)
	defer s.shardedLocks.RUnlock(start)

	for i := start; i < end; i++ {
		vec := s.cache[i]

		if vec == nil {
			vecFromDisk, err := s.handleCacheMiss(ctx, i, false)
			errs[i-start] = err
			vec = vecFromDisk
		}
		out[i-start] = vec
	}

	return out, errs, start, end
}

func (s *shardedLockCache[T]) PageSize() uint64 {
	return s.shardedLocks.PageSize
}

var prefetchFunc func(in uintptr) = func(in uintptr) {
	// do nothing on default arch
	// this function will be overridden for amd64
}

func (s *shardedLockCache[T]) LockAll() {
	s.shardedLocks.LockAll()
}

func (s *shardedLockCache[T]) UnlockAll() {
	s.shardedLocks.UnlockAll()
}

func (s *shardedLockCache[T]) Prefetch(id uint64) {
	s.shardedLocks.RLock(id)
	defer s.shardedLocks.RUnlock(id)

	prefetchFunc(uintptr(unsafe.Pointer(&s.cache[id])))
}

func (s *shardedLockCache[T]) Preload(id uint64, vec []T) {
	s.shardedLocks.Lock(id)
	defer s.shardedLocks.Unlock(id)

	atomic.AddInt64(&s.count, 1)
	s.cache[id] = vec
}

func (s *shardedLockCache[T]) PreloadNoLock(id uint64, vec []T) {
	s.cache[id] = vec
}

func (s *shardedLockCache[T]) SetSizeAndGrowNoLock(size uint64) {
	atomic.StoreInt64(&s.count, int64(size))

	if size < uint64(len(s.cache)) {
		return
	}
	newSize := size + MinimumIndexGrowthDelta
	newCache := make([][]T, newSize)
	copy(newCache, s.cache)
	s.cache = newCache
}

func (s *shardedLockCache[T]) Grow(node uint64) {
	s.maintenanceLock.RLock()
	if node < uint64(len(s.cache)) {
		s.maintenanceLock.RUnlock()
		return
	}
	s.maintenanceLock.RUnlock()

	s.maintenanceLock.Lock()
	defer s.maintenanceLock.Unlock()

	// make sure cache still needs growing
	// (it could have grown while waiting for maintenance lock)
	if node < uint64(len(s.cache)) {
		return
	}

	s.shardedLocks.LockAll()
	defer s.shardedLocks.UnlockAll()

	newSize := node + MinimumIndexGrowthDelta
	newCache := make([][]T, newSize)
	copy(newCache, s.cache)
	s.cache = newCache
}

func (s *shardedLockCache[T]) Len() int32 {
	s.maintenanceLock.RLock()
	defer s.maintenanceLock.RUnlock()

	return int32(len(s.cache))
}

func (s *shardedLockCache[T]) CountVectors() int64 {
	return atomic.LoadInt64(&s.count)
}

func (s *shardedLockCache[T]) Drop() {
	s.deleteAllVectors()
	if s.deletionInterval != 0 {
		s.cancel <- true
	}
}

func (s *shardedLockCache[T]) deleteAllVectors() {
	s.shardedLocks.LockAll()
	defer s.shardedLocks.UnlockAll()

	s.logger.WithField("action", "hnsw_delete_vector_cache").
		Debug("deleting full vector cache")
	for i := range s.cache {
		s.cache[i] = nil
	}

	atomic.StoreInt64(&s.count, 0)
}

func (s *shardedLockCache[T]) watchForDeletion() {
	if s.deletionInterval != 0 {
		f := func() {
			t := time.NewTicker(s.deletionInterval)
			defer t.Stop()
			for {
				select {
				case <-s.cancel:
					return
				case <-t.C:
					s.replaceIfFull()
				}
			}
		}
		enterrors.GoWrapper(f, s.logger)
	}
}

func (s *shardedLockCache[T]) replaceIfFull() {
	if atomic.LoadInt64(&s.count) >= atomic.LoadInt64(&s.maxSize) {
		s.deleteAllVectors()
	}
}

func (s *shardedLockCache[T]) UpdateMaxSize(size int64) {
	atomic.StoreInt64(&s.maxSize, size)
}

func (s *shardedLockCache[T]) CopyMaxSize() int64 {
	sizeCopy := atomic.LoadInt64(&s.maxSize)
	return sizeCopy
}

// noopCache can be helpful in debugging situations, where we want to
// explicitly pass through each vectorForID call to the underlying vectorForID
// function without caching in between.
type noopCache struct {
	vectorForID common.VectorForID[float32]
}

func NewNoopCache(vecForID common.VectorForID[float32], maxSize int,
	logger logrus.FieldLogger,
) *noopCache {
	return &noopCache{vectorForID: vecForID}
}
