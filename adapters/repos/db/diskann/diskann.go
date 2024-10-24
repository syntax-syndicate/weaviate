package diskann

import (
	"math"
	"math/rand"
	"slices"
	"sort"

	"github.com/hashicorp/go-set/v3"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"

	"github.com/muesli/clusters"
	"github.com/muesli/kmeans"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type DiskANNIndex struct {
	PqVectors []PQVector
	mf        *MappedFile
	medoid    *VamanaSegment
	chunkSize int64
}

func NewDiskANNIndex(vectors [][]float64) DiskANNIndex {
	var pqVectors []PQVector
	var ids []uint64
	for i := range vectors {
		pqVectors = append(pqVectors, PQVector{id: uint64(i), vector: makePQVectors(vectors)[i]})
		ids = append(ids, uint64(i))
	}

	segments := BuildFinalGraph(ids, vectors, 2, 10, 10)

	chunkSize := len(vectors[0])*64 + 1024 // rough estimate in bytes

	mf, err := NewMappedFile("test.bin", 1024*1024)
	if err != nil {
		panic(err)
	}

	err = WriteSegmentsToDisk(segments, int64(chunkSize), mf)
	if err != nil {
		panic(err)
	}

	return DiskANNIndex{PqVectors: pqVectors, mf: mf, medoid: findMedoidFast(segments, 100, 10), chunkSize: int64(chunkSize)}

}

func (index *DiskANNIndex) Search(query []float64, k int) []uint64 {
	return index.BeamSearch(query, k, 100)
}

func compareDisk(a *DiskSegment, b *DiskSegment) int {
	return int(a.id - b.id)
}

func vamanaToDiskSegment(segment VamanaSegment) DiskSegment {
	var neighbors []uint64
	for _, neighbor := range segment.neighbors {
		neighbors = append(neighbors, neighbor.id)
	}

	return DiskSegment{id: segment.id, vector: segment.vector, neighbors: neighbors}
}

func (index *DiskANNIndex) BeamSearch(query []float64, k int, searchListSize int) []uint64 {
	results := set.NewTreeSet[*DiskSegment](compareDisk)

	medoid := vamanaToDiskSegment(*index.medoid)

	results.Insert(&medoid)
	visited := set.NewTreeSet[*DiskSegment](compareDisk)

	for !results.Difference(visited).Empty() {
		var p *DiskSegment
		min := math.MaxFloat64
		for _, v := range results.Difference(visited).Slice() {

			if euclideanDistance(query, v.vector) < min {
				min = euclideanDistance(query, v.vector)
				p = v
			}
		}

		var neighbors []*DiskSegment

		var sortedNeighbors []uint64

		slices.SortFunc(p.neighbors, func(i, j uint64) int {

			if L2FloatBytePureGo(query, index.PqVectors[j].vector) < L2FloatBytePureGo(query, index.PqVectors[i].vector) {
				return -1
			}
			return 1
		})

		// optimization: don't load all neighbors into memory, use pq vectors
		for _, v := range sortedNeighbors[:searchListSize-results.Size()] {
			diskNeighbors, _ := ReadSegmentFromDisk(v, index.chunkSize, index.mf)
			neighbors = append(neighbors, &diskNeighbors)

		}

		results.InsertSlice(neighbors)

		visited.Insert(p)

		if results.Size() > searchListSize {
			// retain closest searchListSize elements to query
			newResults := results.Slice()

			sort.Slice(newResults, func(i, j int) bool {
				return euclideanDistance(query, newResults[i].vector) < euclideanDistance(query, newResults[j].vector)
			})

			results = set.TreeSetFrom(newResults[:searchListSize], compareDisk)

		}
	}

	topKResults := results.Slice()
	sort.Slice(topKResults, func(i, j int) bool {
		return euclideanDistance(query, topKResults[i].vector) < euclideanDistance(query, topKResults[j].vector)
	})

	var resultIds []uint64
	for _, v := range topKResults[:k] {
		resultIds = append(resultIds, v.id)
	}

	return resultIds
}

func L2FloatBytePureGo(a []float64, b []byte) float64 {
	var sum float64

	for i := range a {
		diff := a[i] - float64(b[i])
		sum += diff * diff
	}

	return sum
}

type DiskSegment struct {
	id        uint64
	vector    []float64
	neighbors []uint64
}

func makePQVectors(vectors [][]float64) [][]byte {

	// make f32 version of vectors
	vectors_f32 := make([][]float32, len(vectors))
	for i := range vectors {
		vectors_f32[i] = make([]float32, len(vectors[i]))
		for j := range vectors[i] {
			vectors_f32[i][j] = float32(vectors[i][j])
		}
	}

	dimensions := len(vectors[0])
	cfg := ent.PQConfig{
		Enabled: true,
		Encoder: ent.PQEncoder{
			Type:         ent.PQEncoderTypeKMeans,
			Distribution: ent.PQEncoderDistributionLogNormal,
		},
		Centroids: 255,
		Segments:  dimensions,
	}
	distanceProvider := distancer.NewL2SquaredProvider()
	var logger, _ = test.NewNullLogger()
	pq, _ := compressionhelpers.NewProductQuantizer(
		cfg,
		distanceProvider,
		dimensions,
		logger,
	)

	pq.Fit(vectors_f32)
	encoded := make([][]byte, len(vectors))
	for i := 0; i < len(vectors); i++ {
		encoded[i] = pq.Encode(vectors_f32[i])
	}

	return encoded

}

type PQVector struct {
	id     uint64
	vector []byte
}

type VamanaSegment struct {
	id        uint64
	vector    []float64
	neighbors []*VamanaSegment
}

func compare(a *VamanaSegment, b *VamanaSegment) int {
	return int(a.id - b.id)
}

func VamanaBuild(segments []*VamanaSegment, alpha float32, degreeBound int) {

	startingNode := findMedoidFast(segments, 100, 10)

	for _, p := range segments {
		p.neighbors = randomSampleGeneric(degreeBound, degreeBound, segments)

	}

	for _, p := range segments {
		_, vP := greedySearch(startingNode, p.vector, 10, 100)

		robustPrune(segments, p, vP, alpha, degreeBound)

	}

	// add backward edges
	for _, p := range segments {
		for _, v := range p.neighbors {
			for _, v2 := range v.neighbors {
				if v2.id == p.id {
					continue
				}
				v2.neighbors = append(v2.neighbors, v)
				// prune if degree is too high
				if len(v2.neighbors) > degreeBound {
					robustPrune(segments, v2, v2.neighbors, alpha, degreeBound)
				}
			}
		}
	}

}

func BuildFinalGraph(ids []uint64, vectors [][]float64, alpha float32, degreeBound int, k int) []*VamanaSegment {

	var segments []*VamanaSegment

	for i := range vectors {
		segments = append(segments, &VamanaSegment{id: ids[i], vector: vectors[i]})
	}

	_, clusterCenters := kMeans(segments, k)

	topN := 2

	clusterSubgraphs := make([][]*VamanaSegment, len(clusterCenters))

	for _, seg := range segments {

		type ClusterCenterCurrent struct {
			center []float64
			id     int
		}

		var clusterCenters_current []ClusterCenterCurrent

		for i, cc := range clusterCenters {
			clusterCenters_current = append(clusterCenters_current, ClusterCenterCurrent{center: cc, id: i})
		}

		sort.Slice(clusterCenters_current, func(i, j int) bool {
			return euclideanDistance(seg.vector, clusterCenters[i]) < euclideanDistance(seg.vector, clusterCenters[j])
		})

		clusterCenters_current = clusterCenters_current[:topN]

		for _, cc := range clusterCenters_current {
			clusterSubgraphs[cc.id] = append(clusterSubgraphs[cc.id], seg)
		}

	}

	for _, ccSg := range clusterSubgraphs {
		VamanaBuild(ccSg, alpha, degreeBound)
	}

	// merge graphs

	finalGraph := []*VamanaSegment{}

	for _, ccSg := range clusterSubgraphs {
		for _, seg := range ccSg {
			found := false
			for _, finalGraphSeg := range finalGraph {
				// if already in final graph, append to neighbors
				if finalGraphSeg.id == seg.id {
					if !slices.ContainsFunc(finalGraphSeg.neighbors, func(x *VamanaSegment) bool { return x.id == seg.id }) {
						finalGraphSeg.neighbors = append(finalGraphSeg.neighbors, seg)
					}
					found = true
				}
			}
			if !found {
				finalGraph = append(finalGraph, seg)
			}
		}
	}

	return finalGraph

}

func kMeans(segments []*VamanaSegment, k int) ([]int, [][]float64) {
	var d clusters.Observations

	for _, p := range segments {
		d = append(d, clusters.Coordinates(p.vector))

	}

	km := kmeans.New()
	clusters, err := km.Partition(d, k)

	if err != nil {
		panic(err)
	}

	var clusterIds []int
	var clusterCenters [][]float64

	for _, p := range segments {
		for i, cluster := range clusters {
			for _, cle := range cluster.Observations {
				if sliceEqual(p.vector, cle.Coordinates()) {
					clusterIds = append(clusterIds, i)
				}
			}

			clusterCenters = append(clusterCenters, cluster.Center)
		}
	}

	if len(clusterIds) != len(segments) {
		panic("clusterIds and segments are not the same length")
	}

	return clusterIds, clusterCenters

}

func sliceEqual(a, b []float64) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func robustPrune(graph []*VamanaSegment, p *VamanaSegment, candidates []*VamanaSegment, alpha float32, degreeBound int) {

	candidateSet := set.NewTreeSet[*VamanaSegment](compare)

	candidateSet.InsertSlice(p.neighbors)

	candidateSet.Remove(p)

	for !candidateSet.Empty() {
		p_, _ := findClosest(graph, p)

		p.neighbors = append(p.neighbors, p_)

		if len(p.neighbors) > degreeBound {
			break
		}

		for _, p__ := range candidateSet.Slice() {
			if (float64(alpha) * euclideanDistance(p.vector, p_.vector)) <= euclideanDistance(p.vector, p__.vector) {

				candidateSet.Remove(p__)
			}
		}

	}

}

func findClosest(graph []*VamanaSegment, p *VamanaSegment) (*VamanaSegment, float32) {

	var minSegment *VamanaSegment

	min := math.MaxFloat32
	for _, v := range graph {
		if euclideanDistance(p.vector, v.vector) < min {
			min = euclideanDistance(p.vector, v.vector)
			minSegment = v
		}
	}

	return minSegment, float32(min)

}

func greedySearch(start *VamanaSegment, query []float64, k int, searchListSize int) ([]*VamanaSegment, []*VamanaSegment) {
	results := set.NewTreeSet[*VamanaSegment](compare)

	results.Insert(start)
	visited := set.NewTreeSet[*VamanaSegment](compare)

	for !results.Difference(visited).Empty() {
		var p *VamanaSegment
		min := math.MaxFloat64
		for _, v := range results.Difference(visited).Slice() {
			if euclideanDistance(query, v.vector) < min {
				min = euclideanDistance(query, v.vector)
				p = v
			}
		}

		results.InsertSlice(p.neighbors)

		visited.Insert(p)

		if results.Size() > searchListSize {
			// retain closest searchListSize elements to query
			newResults := results.Slice()

			sort.Slice(newResults, func(i, j int) bool {
				return euclideanDistance(query, newResults[i].vector) < euclideanDistance(query, newResults[j].vector)
			})

			results = set.TreeSetFrom(newResults[:searchListSize], compare)

		}
	}

	topKResults := results.Slice()
	sort.Slice(topKResults, func(i, j int) bool {
		return euclideanDistance(query, topKResults[i].vector) < euclideanDistance(query, topKResults[j].vector)
	})

	return topKResults[:k], visited.Slice()

}

func findMedoidFast(segments []*VamanaSegment, sampleSize int, numTrials int) *VamanaSegment {
	if len(segments) == 0 {
		return &VamanaSegment{} // empty segment
	}
	if len(segments) == 1 {
		return segments[0]
	}

	// Adjust sample size if larger than dataset
	if sampleSize > len(segments) {
		sampleSize = len(segments)
	}

	bestMedoid := segments[0]
	bestTotalDistance := math.MaxFloat64

	// Run multiple trials with different random samples
	for trial := 0; trial < numTrials; trial++ {
		// Get random sample
		sampleIndices := randomSample(len(segments), sampleSize)

		// Find best medoid in sample
		minTotalDistance := math.MaxFloat64
		localBestMedoid := segments[0]

		// For each point in sample
		for _, i := range sampleIndices {
			totalDistance := 0.0
			candidateMedoid := segments[i]

			// Calculate distance to ALL points (not just sample)
			// Using early stopping if we exceed current best
			for j := range segments {
				if segments[j].id == candidateMedoid.id {
					continue
				}
				distance := euclideanDistance(candidateMedoid.vector, segments[j].vector)
				totalDistance += distance

				// Early stopping if we exceed current best
				if totalDistance > minTotalDistance {
					break
				}
			}

			if totalDistance < minTotalDistance {
				minTotalDistance = totalDistance
				localBestMedoid = candidateMedoid
			}
		}

		// Update global best if this trial found better medoid
		if minTotalDistance < bestTotalDistance {
			bestTotalDistance = minTotalDistance
			bestMedoid = localBestMedoid
		}
	}

	return bestMedoid
}

// euclideanDistance calculates the Euclidean distance between two vectors
func euclideanDistance(v1, v2 []float64) float64 {
	if len(v1) != len(v2) {
		return math.MaxFloat64
	}

	var sum float64
	for i := 0; i < len(v1); i++ {
		diff := float64(v1[i] - v2[i])
		sum += diff * diff
	}
	return math.Sqrt(sum)
}

func randomSampleGeneric[T any](max, n int, slice []T) []T {

	if n > max {
		n = max
	}

	// Use map to track selected indices
	selected := make(map[int]bool)
	result := make([]T, 0, n)

	for len(result) < n {
		idx := rand.Intn(max)
		if !selected[idx] {
			selected[idx] = true
			result = append(result, slice[idx])
		}
	}
	return result
}

// randomSample returns n random indices from range [0,max)
func randomSample(max, n int) []int {
	if n > max {
		n = max
	}

	// Use map to track selected indices
	selected := make(map[int]bool)
	result := make([]int, 0, n)

	for len(result) < n {
		idx := rand.Intn(max)
		if !selected[idx] {
			selected[idx] = true
			result = append(result, idx)
		}
	}

	return result
}
