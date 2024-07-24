package cache

import (
	"context"
	"math/rand"
	"sync"
	"testing"
)

func TestShardedPageCache(t *testing.T) {
	t.Run("Basic Operations", func(t *testing.T) {
		cache := NewShardedPageCache[int](1000, func(ctx context.Context, id uint64) ([]int, error) {
			return []int{int(id)}, nil
		})

		// Test Preload and Get
		cache.Preload(1, []int{100})
		vec, err := cache.Get(context.Background(), 1)
		if err != nil || vec[0] != 100 {
			t.Errorf("Expected [100], got %v with error %v", vec, err)
		}

		// Test cache miss
		vec, err = cache.Get(context.Background(), 2)
		if err != nil || vec[0] != 2 {
			t.Errorf("Expected [2], got %v with error %v", vec, err)
		}

		// Test Delete
		cache.Delete(context.Background(), 1)
		_, err = cache.Get(context.Background(), 1)
		if err != nil {
			t.Errorf("Expected no error after delete and re-get, got %v", err)
		}
	})

	t.Run("Concurrent Operations", func(t *testing.T) {
		cache := NewShardedPageCache[int](10000, func(ctx context.Context, id uint64) ([]int, error) {
			return []int{int(id)}, nil
		})

		var wg sync.WaitGroup
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func(id uint64) {
				defer wg.Done()
				cache.Preload(id, []int{int(id)})
				vec, err := cache.Get(context.Background(), id)
				if err != nil || vec[0] != int(id) {
					t.Errorf("Concurrent operation failed for id %d", id)
				}
			}(uint64(i))
		}
		wg.Wait()

		if cache.Len() != 1000 {
			t.Errorf("Expected 1000 items in cache, got %d", cache.Len())
		}
	})

	t.Run("IterateVectors", func(t *testing.T) {
		cache := NewShardedPageCache[int](1000, func(ctx context.Context, id uint64) ([]int, error) {
			return []int{int(id)}, nil
		})

		for i := 0; i < 100; i++ {
			cache.Preload(uint64(i), []int{i})
		}

		count := 0
		err := cache.IterateVectors(context.Background(), func(id uint64, vec []int) bool {
			count++
			return true
		})

		if err != nil {
			t.Errorf("IterateVectors returned error: %v", err)
		}

		if count != 100 {
			t.Errorf("Expected to iterate over 100 items, got %d", count)
		}
	})

	t.Run("MultiGet", func(t *testing.T) {
		cache := NewShardedPageCache[int](1000, func(ctx context.Context, id uint64) ([]int, error) {
			return []int{int(id)}, nil
		})

		ids := make([]uint64, 100)
		for i := range ids {
			ids[i] = uint64(i)
			cache.Preload(uint64(i), []int{i})
		}

		results, errors := cache.MultiGet(context.Background(), ids)

		for i, vec := range results {
			if errors[i] != nil {
				t.Errorf("MultiGet returned error for id %d: %v", i, errors[i])
			}
			if vec[0] != i {
				t.Errorf("Expected [%d], got %v for id %d", i, vec, i)
			}
		}
	})

	t.Run("Grow and UpdateMaxSize", func(t *testing.T) {
		cache := NewShardedPageCache[int](100, func(ctx context.Context, id uint64) ([]int, error) {
			return []int{int(id)}, nil
		})

		cache.Grow(200)
		if cache.CopyMaxSize() != 200 {
			t.Errorf("Expected maxSize 200, got %d", cache.CopyMaxSize())
		}

		cache.UpdateMaxSize(300)
		if cache.CopyMaxSize() != 300 {
			t.Errorf("Expected maxSize 300, got %d", cache.CopyMaxSize())
		}
	})

	t.Run("Drop", func(t *testing.T) {
		cache := NewShardedPageCache[int](1000, func(ctx context.Context, id uint64) ([]int, error) {
			return []int{int(id)}, nil
		})

		for i := 0; i < 100; i++ {
			cache.Preload(uint64(i), []int{i})
		}

		cache.Drop()

		if cache.Len() != 0 {
			t.Errorf("Expected 0 items after Drop, got %d", cache.Len())
		}
	})

	t.Run("Large Scale Test", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping large scale test in short mode")
		}

		cache := NewShardedPageCache[int](1000000, func(ctx context.Context, id uint64) ([]int, error) {
			return []int{int(id)}, nil
		})

		var wg sync.WaitGroup
		for i := 0; i < 100000; i++ {
			wg.Add(1)
			go func(id uint64) {
				defer wg.Done()
				cache.Preload(id, []int{int(id)})
			}(uint64(i))
		}
		wg.Wait()

		if cache.Len() != 100000 {
			t.Errorf("Expected 100000 items in cache, got %d", cache.Len())
		}

		// Test random access
		for i := 0; i < 10000; i++ {
			id := rand.Uint64() % 100000
			vec, err := cache.Get(context.Background(), id)
			if err != nil || vec[0] != int(id) {
				t.Errorf("Random access failed for id %d", id)
			}
		}
	})
}

func TestSimpleLoadAndDelete(t *testing.T) {
	const numEntries = 10000

	cache := NewShardedPageCache[int](20000, func(ctx context.Context, id uint64) ([]int, error) {
		return []int{int(id)}, nil
	})

	// Step 1: Load entries
	t.Run("Load Entries", func(t *testing.T) {
		for i := 0; i < numEntries; i++ {
			cache.Preload(uint64(i), []int{i})
		}

		if cache.Len() != numEntries {
			t.Errorf("Expected cache size %d, got %d", numEntries, cache.Len())
		}

		// Verify all entries are in the cache
		for i := 0; i < numEntries; i++ {
			vec, err := cache.Get(context.Background(), uint64(i))
			if err != nil {
				t.Errorf("Failed to get entry %d: %v", i, err)
			}
			if len(vec) != 1 || vec[0] != i {
				t.Errorf("Incorrect value for entry %d: expected [%d], got %v", i, i, vec)
			}
		}
	})

	// Step 2: Delete entries
	t.Run("Delete Entries", func(t *testing.T) {
		for i := 0; i < numEntries; i++ {
			cache.Delete(context.Background(), uint64(i))

			// Check cache size after each deletion
			expectedSize := numEntries - i - 1
			if cache.Len() != int32(expectedSize) {
				t.Errorf("After deleting %d, expected cache size %d, got %d", i, expectedSize, cache.Len())
				// Break early if we detect an issue
				break
			}
		}

		// Verify final cache state
		if cache.Len() != 0 {
			t.Errorf("Expected empty cache, got size %d", cache.Len())
		}
	})

	// Step 3: Verify cache state
	t.Run("Verify Final State", func(t *testing.T) {
		var count int
		cache.IterateVectors(context.Background(), func(id uint64, vec []int) bool {
			count++
			t.Errorf("Unexpected entry in cache: id=%d, vec=%v", id, vec)
			return count < 10 // Limit the number of errors we print
		})

		if count > 0 {
			t.Errorf("Found %d unexpected entries in the cache", count)
		}
	})
}

func TestDeterministicConcurrentCacheOperations(t *testing.T) {
	const (
		numIDs        = 10000
		numGoroutines = 100
		deleteRatio   = 0.3 // 30% of IDs will be deleted
	)

	// Create a slice of IDs
	ids := make([]uint64, numIDs)
	for i := range ids {
		ids[i] = uint64(i)
	}

	// Create cache
	cache := NewShardedPageCache[int](int64(numIDs), func(ctx context.Context, id uint64) ([]int, error) {
		return []int{int(id)}, nil
	})

	// Concurrent Preload
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for j := start; j < numIDs; j += numGoroutines {
				cache.Preload(ids[j], []int{int(ids[j])})
			}
		}(i)
	}
	wg.Wait()

	// Verify all IDs are in the cache
	for _, id := range ids {
		vec, err := cache.Get(context.Background(), id)
		if err != nil || len(vec) != 1 || vec[0] != int(id) {
			t.Errorf("After Preload: Expected entry %d not found or incorrect", id)
		}
	}

	// Create a set of IDs to delete
	deleteSet := make(map[uint64]bool)
	for i, id := range ids {
		if float64(i)/float64(numIDs) < deleteRatio {
			deleteSet[id] = true
		}
	}

	// Concurrent Delete
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for j := start; j < numIDs; j += numGoroutines {
				if deleteSet[ids[j]] {
					cache.Delete(context.Background(), ids[j])
				}
			}
		}(i)
	}
	wg.Wait()

	// Verify final state
	expectedCount := numIDs - len(deleteSet)
	actualCount := int(cache.Len())

	t.Logf("Expected entries: %d", expectedCount)
	t.Logf("Actual entries: %d", actualCount)

	if actualCount != expectedCount {
		t.Errorf("Cache size mismatch. Expected: %d, Got: %d", expectedCount, actualCount)
	}

	// Check that all non-deleted entries are in the cache
	missingEntries := 0
	for _, id := range ids {
		if !deleteSet[id] {
			vec, err := cache.Get(context.Background(), id)
			if err != nil || len(vec) != 1 || vec[0] != int(id) {
				missingEntries++
			}
		}
	}
	if missingEntries > 0 {
		t.Errorf("Found %d missing entries in the cache", missingEntries)
	}

	// Check that no deleted entries are in the cache
	unexpectedEntries := 0
	cache.IterateVectors(context.Background(), func(id uint64, vec []int) bool {
		if deleteSet[id] {
			unexpectedEntries++
		}
		return true
	})

	if unexpectedEntries > 0 {
		t.Errorf("Found %d unexpected entries in the cache", unexpectedEntries)
	}
}

func BenchmarkShardedPageCache(b *testing.B) {
	cache := NewShardedPageCache[int](1000000, func(ctx context.Context, id uint64) ([]int, error) {
		return []int{int(id)}, nil
	})

	b.Run("Preload", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cache.Preload(uint64(i), []int{i})
		}
	})

	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = cache.Get(context.Background(), uint64(i%1000000))
		}
	})

	b.Run("Concurrent Get", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				id := rand.Uint64() % 1000000
				_, _ = cache.Get(context.Background(), id)
			}
		})
	})

	b.Run("IterateVectors", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = cache.IterateVectors(context.Background(), func(id uint64, vec []int) bool {
				return true
			})
		}
	})
}
