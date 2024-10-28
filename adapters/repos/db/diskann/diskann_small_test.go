package diskann

import (
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"sort"
	"testing"
	"time"
)

type TestData struct {
	vectors     [][]float32
	externalIds []uint64
	queries     [][]float32
	groundTruth [][]uint64
}

func generateTestData(numVectors, dimensions, numQueries, k int) TestData {
	// Generate random vectors
	vectors := make([][]float32, numVectors)
	externalIds := make([]uint64, numVectors)
	for i := range vectors {
		vectors[i] = make([]float32, dimensions)
		for j := range vectors[i] {
			vectors[i][j] = rand.Float32()
		}
		externalIds[i] = uint64(i)
	}

	// Generate query vectors
	queries := make([][]float32, numQueries)
	for i := range queries {
		queries[i] = make([]float32, dimensions)
		for j := range queries[i] {
			queries[i][j] = rand.Float32()
		}
	}

	// Calculate ground truth
	groundTruth := make([][]uint64, numQueries)
	for i, query := range queries {
		// Calculate distances to all vectors
		type distanceIndex struct {
			distance float32
			id       uint64
		}
		distances := make([]distanceIndex, numVectors)
		for j := range vectors {
			distances[j] = distanceIndex{
				distance: euclideanDistance(query, vectors[j]),
				id:       externalIds[j],
			}
		}

		// Sort by distance
		sort.Slice(distances, func(a, b int) bool {
			return distances[a].distance < distances[b].distance
		})

		// Store top k results
		groundTruth[i] = make([]uint64, k)
		for j := 0; j < k; j++ {
			groundTruth[i][j] = distances[j].id
		}
	}

	return TestData{
		vectors:     vectors,
		externalIds: externalIds,
		queries:     queries,
		groundTruth: groundTruth,
	}
}

func TestDiskANNRecall(t *testing.T) {
	// Create a CPU profile file
	f, err := os.Create("cpu_profile.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close()

	// Start CPU profiling
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()
	// Test parameters
	numVectors := 5_000
	dimensions := 128
	k := 10 // top-k for search
	numQueries := 1_000

	// Generate test data
	testData := generateTestData(numVectors, dimensions, numQueries, k)

	// Build index and measure build time
	buildStart := time.Now()
	index := NewDiskANNIndex(testData.vectors, testData.externalIds)
	buildTime := time.Since(buildStart)

	// Perform searches and measure times
	var totalRecall float64
	var totalQueryTime time.Duration

	for i, query := range testData.queries {
		queryStart := time.Now()
		results := index.Search(query, k)
		totalQueryTime += time.Since(queryStart)

		// Calculate recall for this query
		recall := calculateRecall(results, testData.groundTruth[i])
		totalRecall += recall
	}

	avgRecall := totalRecall / float64(numQueries)
	avgQueryTime := totalQueryTime / time.Duration(numQueries)
	qps := float64(numQueries) / totalQueryTime.Seconds()

	// Print results
	t.Logf("Build time: %v", buildTime)
	t.Logf("Average query time: %v", avgQueryTime)
	t.Logf("QPS: %.2f", qps)
	t.Logf("Average recall@%d: %.4f", k, avgRecall)

	// Assert minimum recall threshold
	minRecallThreshold := 0.8
	if avgRecall < minRecallThreshold {
		t.Errorf("Average recall %.4f below minimum threshold %.4f", avgRecall, minRecallThreshold)
	}
}

// calculateRecall computes the recall between result and ground truth
func calculateRecall(result, groundTruth []uint64) float64 {
	matches := 0
	groundTruthSet := make(map[uint64]struct{})

	for _, id := range groundTruth {
		groundTruthSet[id] = struct{}{}
	}

	for _, id := range result {
		if _, exists := groundTruthSet[id]; exists {
			matches++
		}
	}

	return float64(matches) / float64(len(groundTruth))
}
