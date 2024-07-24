package cache

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	NumShards       = 512
	InitialPageSize = 1024
	MaxPageSize     = 4096
)

type Entry[T any] struct {
	id  uint64
	vec []T
}

type Page[T any] struct {
	entries []Entry[T]
	minID   uint64
	maxID   uint64
}

type Shard[T any] struct {
	pages []Page[T]
	count int64
	mu    sync.RWMutex
}

type ShardedPageCache[T any] struct {
	shards      [NumShards]*Shard[T]
	maxSize     int64
	count       int64
	vectorForID func(ctx context.Context, id uint64) ([]T, error)
}

func NewShardedPageCache[T any](maxSize int64, vectorForID func(ctx context.Context, id uint64) ([]T, error)) *ShardedPageCache[T] {
	c := &ShardedPageCache[T]{
		maxSize:     maxSize,
		vectorForID: vectorForID,
	}
	for i := range c.shards {
		c.shards[i] = &Shard[T]{
			pages: make([]Page[T], 1),
		}
		c.shards[i].pages[0].entries = make([]Entry[T], 0, InitialPageSize)
	}
	return c
}

func (c *ShardedPageCache[T]) shardIndex(id uint64) int {
	return int((id / NumShards) % NumShards)
}

func (c *ShardedPageCache[T]) Get(ctx context.Context, id uint64) ([]T, error) {
	shard := c.shards[c.shardIndex(id)]
	shard.mu.RLock()
	pageIndex := sort.Search(len(shard.pages), func(i int) bool {
		return shard.pages[i].maxID >= id
	})
	if pageIndex < len(shard.pages) {
		page := &shard.pages[pageIndex]
		entryIndex := sort.Search(len(page.entries), func(i int) bool {
			return page.entries[i].id >= id
		})
		if entryIndex < len(page.entries) && page.entries[entryIndex].id == id {
			vec := page.entries[entryIndex].vec
			shard.mu.RUnlock()
			return vec, nil
		}
	}
	shard.mu.RUnlock()

	// Cache miss
	vec, err := c.vectorForID(ctx, id)
	if err != nil {
		return nil, err
	}

	c.Preload(id, vec)
	return vec, nil
}

func (c *ShardedPageCache[T]) Preload(id uint64, vec []T) {
	shard := c.shards[c.shardIndex(id)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	pageIndex := sort.Search(len(shard.pages), func(i int) bool {
		return shard.pages[i].maxID >= id
	})

	if pageIndex < len(shard.pages) {
		page := &shard.pages[pageIndex]
		entryIndex := sort.Search(len(page.entries), func(i int) bool {
			return page.entries[i].id >= id
		})

		if entryIndex < len(page.entries) && page.entries[entryIndex].id == id {
			// Update existing entry
			page.entries[entryIndex].vec = vec
			return
		}

		if len(page.entries) < MaxPageSize {
			// Insert the new entry
			page.entries = append(page.entries, Entry[T]{})
			copy(page.entries[entryIndex+1:], page.entries[entryIndex:])
			page.entries[entryIndex] = Entry[T]{id: id, vec: vec}
			if id < page.minID {
				page.minID = id
			}
			if id > page.maxID {
				page.maxID = id
			}
		} else {
			// Create a new page
			newPage := Page[T]{
				entries: make([]Entry[T], 0, InitialPageSize),
				minID:   id,
				maxID:   id,
			}
			newPage.entries = append(newPage.entries, Entry[T]{id: id, vec: vec})
			shard.pages = append(shard.pages, Page[T]{})
			copy(shard.pages[pageIndex+1:], shard.pages[pageIndex:])
			shard.pages[pageIndex] = newPage
		}
	} else {
		// Append a new page
		newPage := Page[T]{
			entries: make([]Entry[T], 0, InitialPageSize),
			minID:   id,
			maxID:   id,
		}
		newPage.entries = append(newPage.entries, Entry[T]{id: id, vec: vec})
		shard.pages = append(shard.pages, newPage)
	}

	atomic.AddInt64(&shard.count, 1)
	atomic.AddInt64(&c.count, 1)
}

func (c *ShardedPageCache[T]) Delete(ctx context.Context, id uint64) {
	shard := c.shards[c.shardIndex(id)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	pageIndex := sort.Search(len(shard.pages), func(i int) bool {
		return shard.pages[i].maxID >= id
	})

	if pageIndex < len(shard.pages) {
		page := &shard.pages[pageIndex]
		entryIndex := sort.Search(len(page.entries), func(i int) bool {
			return page.entries[i].id >= id
		})

		if entryIndex < len(page.entries) && page.entries[entryIndex].id == id {
			page.entries = append(page.entries[:entryIndex], page.entries[entryIndex+1:]...)
			atomic.AddInt64(&shard.count, -1)
			atomic.AddInt64(&c.count, -1)

			if len(page.entries) == 0 {
				// Remove the empty page
				shard.pages = append(shard.pages[:pageIndex], shard.pages[pageIndex+1:]...)
			} else {
				// Update minID and maxID
				page.minID = page.entries[0].id
				page.maxID = page.entries[len(page.entries)-1].id
			}
		}
	}
}

func (c *ShardedPageCache[T]) IterateVectors(ctx context.Context, f func(id uint64, vec []T) bool) error {
	for _, shard := range c.shards {
		shard.mu.RLock()
		for _, page := range shard.pages {
			for _, entry := range page.entries {
				if !f(entry.id, entry.vec) {
					shard.mu.RUnlock()
					return nil
				}
			}
		}
		shard.mu.RUnlock()
	}
	return nil
}

func (c *ShardedPageCache[T]) Len() int32 {
	return int32(atomic.LoadInt64(&c.count))
}

func (c *ShardedPageCache[T]) Grow(size uint64) {
	atomic.StoreInt64(&c.maxSize, int64(size))
}

func (c *ShardedPageCache[T]) UpdateMaxSize(size int64) {
	atomic.StoreInt64(&c.maxSize, size)
}

func (c *ShardedPageCache[T]) CopyMaxSize() int64 {
	return atomic.LoadInt64(&c.maxSize)
}

func (c *ShardedPageCache[T]) Drop() {
	for _, shard := range c.shards {
		shard.mu.Lock()
		shard.pages = []Page[T]{{
			entries: make([]Entry[T], 0, InitialPageSize),
			minID:   0,
			maxID:   0,
		}}
		atomic.StoreInt64(&shard.count, 0)
		shard.mu.Unlock()
	}
	atomic.StoreInt64(&c.count, 0)
}

func (c *ShardedPageCache[T]) MultiGet(ctx context.Context, ids []uint64) ([][]T, []error) {
	results := make([][]T, len(ids))
	errors := make([]error, len(ids))

	var wg sync.WaitGroup
	wg.Add(len(ids))

	for i, id := range ids {
		go func(index int, vectorID uint64) {
			defer wg.Done()
			results[index], errors[index] = c.Get(ctx, vectorID)
		}(i, id)
	}

	wg.Wait()
	return results, errors
}
