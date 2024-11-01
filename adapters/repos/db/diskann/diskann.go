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

	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type DiskANNIndex struct {
	PqVectors        []PQVector
	mf               *MappedFile
	medoid           *VamanaSegment
	chunkSize        int64
	idsToExternalIds map[uint64]uint64
	vectorLenSize    int64
	neighborLenSize  int64
	pq               *compressionhelpers.ProductQuantizer
}

func NewDiskANNIndex(vectors [][]float32, externalIds []uint64) DiskANNIndex {
	var pqVectors []PQVector
	var ids []uint64

	pqvectors, pq := makePQVectors(vectors)

	idsToExternalIds := make(map[uint64]uint64)

	vectorLenSize := int64(len(vectors[0]) * 4)

	println("making pq vectors")
	for i := range vectors {
		pqVectors = append(pqVectors, PQVector{id: uint64(i), vector: pqvectors[i]})
		ids = append(ids, uint64(i))
		idsToExternalIds[uint64(i)] = externalIds[i]

	}

	println("made pq vectors")

	degreeBound := 128

	neighborLenSize := int64(degreeBound * 8)
	chunkSize := CalculateAlignedChunkSize(len(vectors[0]), degreeBound)

	index := DiskANNIndex{PqVectors: pqVectors, chunkSize: int64(chunkSize), idsToExternalIds: idsToExternalIds, vectorLenSize: vectorLenSize, neighborLenSize: neighborLenSize, pq: pq}

	mf, err := NewMappedFile("test.bin", chunkSize*int64(len(vectors)))
	if err != nil {
		panic(err)
	}

	index.mf = mf
	segments := index.BuildFinalGraph(ids, vectors, 2, degreeBound, 10)
	index.medoid = findMedoidFast(segments, 100, 10)

	err = WriteSegmentsToDisk(segments, int64(chunkSize), mf, vectorLenSize, neighborLenSize)
	if err != nil {
		panic(err)
	}

	return index

}

func CalculateAlignedChunkSize(vectorLen int, maxNeighbors int) int64 {
	// Fixed header sizes
	idSize := int64(8)          // uint64 for ID
	vectorLenSize := int64(4)   // uint32 for vector length
	neighborLenSize := int64(4) // uint32 for neighbors length

	// Data sizes
	vectorSize := int64(vectorLen * 4)       // float32 = 4 bytes each
	neighborsSize := int64(maxNeighbors * 8) // uint64 = 8 bytes each

	// Calculate total raw size
	rawSize := idSize + vectorLenSize + vectorSize + neighborLenSize + neighborsSize

	// Align to 4KB (common SSD page size)
	alignSize := int64(4096)
	alignedSize := ((rawSize + alignSize - 1) / alignSize) * alignSize

	return alignedSize
}

// Example usage:
// chunkSize := CalculateAlignedChunkSize(128, 64) // for 128-dim vectors and max 64 neighbors

func (index *DiskANNIndex) Search(query []float32, k int) []uint64 {
	results := index.BeamSearch(query, k, 50, 8, 4)

	results_external := make([]uint64, k)
	for i, v := range results {
		results_external[i] = index.idsToExternalIds[v]
	}
	return results_external

}

func compareDisk(a *DiskSegment, b *DiskSegment) int {
	return int(a.Id - b.Id)
}

func vamanaToDiskSegment(segment VamanaSegment) DiskSegment {
	var neighbors []uint64
	for _, neighbor := range segment.neighbors {
		neighbors = append(neighbors, neighbor.id)
	}

	return DiskSegment{Id: segment.id, Vector: segment.vector, Neighbors: neighbors}
}

func (index *DiskANNIndex) BeamSearch(query []float32, k int, searchListSize int, beamWidth int, maxWorkers int) []uint64 {
	searchList := set.New[uint64](searchListSize + 50)
	medoid := vamanaToDiskSegment(*index.medoid)
	searchList.Insert(medoid.Id)
	visited := set.New[uint64](100)
	query_byte := index.pq.Encode(query)

	for !searchList.Difference(visited).Empty() {
		// Get beamWidth closest unvisited points
		unvisited := searchList.Difference(visited).Slice()

		slices.SortFunc(unvisited, func(i, j uint64) int {
			distI := index.L2FloatBytePureGo(query_byte, index.PqVectors[i].vector)
			distJ := index.L2FloatBytePureGo(query_byte, index.PqVectors[j].vector)
			if distI < distJ {
				return -1
			} else if distI > distJ {
				return 1
			}
			return 0
		})

		if len(unvisited) > beamWidth {
			unvisited = unvisited[:beamWidth]
		}

		unvisited_diskSegments := make([]DiskSegment, beamWidth)

		for i, id := range unvisited {
			if segment, err := ReadSegmentFromDisk(id, index.chunkSize, index.mf, index.vectorLenSize, index.neighborLenSize); err == nil {
				unvisited_diskSegments[i] = segment
			}
		}

		// Process segments and their neighbors
		for _, segment := range unvisited_diskSegments {
			visited.Insert(segment.Id)

			// Add all neighbors to search list
			for _, neighborId := range segment.Neighbors {
				if !visited.Contains(neighborId) {
					searchList.Insert(neighborId)
				}
			}
		}

		// Prune results if necessary
		if searchList.Size() > searchListSize {
			newResults := searchList.Slice()

			slices.SortFunc(newResults, func(i, j uint64) int {
				distI := index.L2FloatBytePureGo(query_byte, index.PqVectors[i].vector)
				distJ := index.L2FloatBytePureGo(query_byte, index.PqVectors[j].vector)
				if distI < distJ {
					return -1
				} else if distI > distJ {
					return 1
				}
				return 0
			})
			searchList = set.From(newResults[:searchListSize])
		}
	}

	// Get final top k results
	topKResults := searchList.Slice()
	topKResults_diskSegments := make([]DiskSegment, 0, len(topKResults))

	for _, id := range topKResults {
		if segment, err := ReadSegmentFromDisk(id, index.chunkSize, index.mf, index.vectorLenSize, index.neighborLenSize); err == nil {
			topKResults_diskSegments = append(topKResults_diskSegments, segment)
		}
	}

	sort.Slice(topKResults_diskSegments, func(i, j int) bool {
		return euclideanDistance(query, topKResults_diskSegments[i].Vector) < euclideanDistance(query, topKResults_diskSegments[j].Vector)
	})

	// Only now take top k
	resultSize := min(k, len(topKResults_diskSegments))
	resultIds := make([]uint64, resultSize)
	for i := 0; i < resultSize; i++ {
		resultIds[i] = topKResults_diskSegments[i].Id
	}

	return resultIds
}

func (index *DiskANNIndex) L2FloatBytePureGo(a []byte, b []byte) float32 {

	sum, _ := index.pq.DistanceBetweenCompressedVectors(a, b)

	return sum
}

func makePQVectors(vectors [][]float32) ([][]byte, *compressionhelpers.ProductQuantizer) {

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

	println("pq")

	return encoded, pq

}

type PQVector struct {
	id     uint64
	vector []byte
}

type VamanaSegment struct {
	id        uint64
	vector    []float32
	neighbors []*VamanaSegment
}

func compare(a *VamanaSegment, b *VamanaSegment) int {
	return int(a.id - b.id)
}

func VamanaBuild(segments []*VamanaSegment, alpha float32, degreeBound int) {

	println("VamanaBuild\n")

	startingNode := findMedoidFast(segments, 100, 10)

	println("startingNode: ", startingNode.id)

	for _, p := range segments {
		p.neighbors = randomSampleGeneric(degreeBound, degreeBound, segments)

	}

	// Generate random permutation
	indices := make([]int, len(segments))
	for i := range indices {
		indices[i] = i
	}
	rand.Shuffle(len(indices), func(i, j int) {
		indices[i], indices[j] = indices[j], indices[i]
	})

	println("randomSampleGeneric")

	// First pass with alpha = 1
	firstPassAlpha := float32(1.0)
	for _, idx := range indices {
		print("|")
		p := segments[idx]
		_, vP := greedySearch(startingNode, p.vector, 10, 100)
		robustPrune(segments, p, vP, firstPassAlpha, degreeBound)

		// Update backward edges immediately
		for _, neighbor := range p.neighbors {
			// Check if reverse edge already exists
			hasReverseEdge := false
			for _, v := range neighbor.neighbors {
				if v.id == p.id {
					hasReverseEdge = true
					break
				}
			}

			// If no reverse edge, add it
			if !hasReverseEdge {
				neighbor.neighbors = append(neighbor.neighbors, p)

				// Only do RobustPrune if degree now exceeds bound
				if len(neighbor.neighbors) > degreeBound {
					robustPrune(segments, neighbor, neighbor.neighbors, firstPassAlpha, degreeBound)
				}
			}
		}
	}

	println("first pass")

	// Second pass with user alpha
	for _, idx := range indices {
		print("|")
		p := segments[idx]
		_, vP := greedySearch(startingNode, p.vector, 10, 100)
		robustPrune(segments, p, vP, alpha, degreeBound)

		// Update backward edges immediately
		for _, neighbor := range p.neighbors {
			// Check if reverse edge already exists
			hasReverseEdge := false
			for _, v := range neighbor.neighbors {
				if v.id == p.id {
					hasReverseEdge = true
					break
				}
			}

			// If no reverse edge, add it
			if !hasReverseEdge {
				neighbor.neighbors = append(neighbor.neighbors, p)

				// Only do RobustPrune if degree now exceeds bound
				if len(neighbor.neighbors) > degreeBound {
					robustPrune(segments, neighbor, neighbor.neighbors, firstPassAlpha, degreeBound)
				}
			}
		}
	}

	println("second pass")

	for _, s := range segments {
		println(s.id)
		for _, n := range s.neighbors {
			print(n.id)
			print(" ")
		}
	}

	println("done")

}

func (index *DiskANNIndex) BuildFinalGraph(ids []uint64, vectors [][]float32, alpha float32, degreeBound int, k int) []*VamanaSegment {

	var segments []*VamanaSegment

	for i := range vectors {
		segments = append(segments, &VamanaSegment{id: ids[i], vector: vectors[i]})
	}
	finalGraph := []*VamanaSegment{}

	// split graph build if too large
	if len(segments) >= 5_000 {

		kMeans := compressionhelpers.NewKMeans(k, len(segments[0].vector), 0)

		kMeans.Fit(vectors)

		println("split into clusters: ", k)

		topN := 2

		clusterSubgraphs := make([][]*VamanaSegment, k)

		for _, seg := range segments {

			clusterCenters := kMeans.NNearest(seg.vector, topN)

			for _, center := range clusterCenters {
				clusterSubgraphs[center] = append(clusterSubgraphs[center], seg)
			}

		}

		// first := true

		written := make(map[uint64]bool)

		for _, ccSg := range clusterSubgraphs {
			println("cluster size: ", len(ccSg))
			VamanaBuild(ccSg, alpha, degreeBound)
			// if first {
			// 	WriteSegmentsToDisk(ccSg, int64(index.chunkSize), index.mf, index.vectorLenSize, index.neighborLenSize)
			// first = false
			// } else {
			for _, seg := range ccSg {

				var ds DiskSegment
				if !written[seg.id] {
					ds = vamanaToDiskSegment(*seg)
					written[seg.id] = true
				} else {
					ds, _ = ReadSegmentFromDisk(seg.id, int64(index.chunkSize), index.mf, index.vectorLenSize, index.neighborLenSize)
					// fails before here in disk.go
					for _, n := range seg.neighbors {
						if !slices.Contains(ds.Neighbors, n.id) {
							ds.Neighbors = append(ds.Neighbors, n.id)
						}
					}
				}
				WriteSegmentToDisk(ds, int64(index.chunkSize), index.mf, index.vectorLenSize, index.neighborLenSize)
			}
			// }
		}

	} else {
		VamanaBuild(segments, alpha, degreeBound)

		finalGraph = segments

		WriteSegmentsToDisk(finalGraph, int64(index.chunkSize), index.mf, index.vectorLenSize, index.neighborLenSize)

	}

	return finalGraph

}

func kMeans(segments []*VamanaSegment, k int) ([]uint64, [][]float32) {

	kmeans := compressionhelpers.NewKMeans(k, len(segments[0].vector), 0)

	vectors := make([][]float32, len(segments))
	for i, segment := range segments {

		vectors[i] = make([]float32, len(segment.vector))
		for j := range segment.neighbors {
			vectors[i][j] = float32(segment.vector[j])
		}
	}
	kmeans.Fit(vectors)

	clusterIds := make([]uint64, len(segments))

	for i, _ := range clusterIds {
		clusterIds[i] = kmeans.Nearest(vectors[i])
	}

	kMeansCenters := kmeans.Centers()
	clusterCenters := make([][]float32, len(kMeansCenters))

	for i, _ := range clusterCenters {
		clusterCenters[i] = make([]float32, len(segments[0].vector))
		for j := range clusterCenters[i] {
			clusterCenters[i][j] = float32(kMeansCenters[i][j])
		}
	}

	return clusterIds, clusterCenters

}

func sliceEqual(a, b []float32) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func robustPrune(graph []*VamanaSegment, p *VamanaSegment, candidates []*VamanaSegment, alpha float32, degreeBound int) {

	candidateSet := set.NewTreeSet[*VamanaSegment](compare)
	candidateSet.InsertSlice(candidates)
	candidateSet.InsertSlice(p.neighbors)

	candidateSet.Remove(p)

	p.neighbors = []*VamanaSegment{}

	for !candidateSet.Empty() {
		p_, _ := findClosest(candidateSet.Slice(), p)

		p.neighbors = append(p.neighbors, p_)

		if len(p.neighbors) >= degreeBound {
			break
		}

		for _, p__ := range candidateSet.Slice() {
			if (float32(alpha) * euclideanDistance(p_.vector, p__.vector)) <= euclideanDistance(p.vector, p__.vector) {

				candidateSet.Remove(p__)
			}
		}

	}

}

func findClosest(graph []*VamanaSegment, p *VamanaSegment) (*VamanaSegment, float32) {

	var minSegment *VamanaSegment

	min := float32(math.MaxFloat32)
	for _, v := range graph {
		if euclideanDistance(p.vector, v.vector) < min {
			min = euclideanDistance(p.vector, v.vector)
			minSegment = v
		}
	}

	return minSegment, float32(min)

}

func greedySearch(start *VamanaSegment, query []float32, k int, searchListSize int) ([]*VamanaSegment, []*VamanaSegment) {
	results := set.NewTreeSet[*VamanaSegment](compare)

	results.Insert(start)
	visited := set.NewTreeSet[*VamanaSegment](compare)

	for !results.Difference(visited).Empty() {
		var p *VamanaSegment
		min := float32(math.MaxFloat32)
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

			slices.SortFunc(newResults, func(i, j *VamanaSegment) int {
				distI := euclideanDistance(query, i.vector)
				distJ := euclideanDistance(query, j.vector)
				if distI < distJ {
					return -1
				} else if distI > distJ {
					return 1
				}
				return 0
			})

			results = set.TreeSetFrom(newResults[:searchListSize], compare)

		}
	}

	topKResults := results.Slice()

	slices.SortFunc(topKResults, func(i, j *VamanaSegment) int {
		distI := euclideanDistance(query, i.vector)
		distJ := euclideanDistance(query, j.vector)
		if distI < distJ {
			return -1
		} else if distI > distJ {
			return 1
		}
		return 0
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
	bestTotalDistance := float32(math.MaxFloat32)

	// Run multiple trials with different random samples
	for trial := 0; trial < numTrials; trial++ {
		// Get random sample
		sampleIndices := randomSample(len(segments), sampleSize)

		// Find best medoid in sample
		minTotalDistance := float32(math.MaxFloat32)
		localBestMedoid := segments[0]

		// For each point in sample
		for _, i := range sampleIndices {
			totalDistance := float32(0.0)
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
func euclideanDistance(v1, v2 []float32) float32 {

	l2provider := distancer.NewL2SquaredProvider()
	if len(v1) != len(v2) {
		return math.MaxFloat32
	}

	result, _ := l2provider.SingleDist(v1, v2)
	return result
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
