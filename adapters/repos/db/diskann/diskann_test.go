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

package diskann

import (
	"errors"
	"log"
	"math"
	"testing"
	"time"

	"github.com/weaviate/hdf5"
)

func getHDF5ByteSize(dataset *hdf5.Dataset) (uint, error) {

	datatype, err := dataset.Datatype()
	if err != nil {
		return 0, errors.New("Unabled to read datatype\n")
	}

	// log.WithFields(log.Fields{"size": datatype.Size()}).Printf("Parsing HDF5 byte format\n")
	byteSize := datatype.Size()
	if byteSize != 4 && byteSize != 8 {
		return 0, errors.New("Unable to load dataset with byte size")
	}
	return byteSize, nil
}

func convert1DChunk[D float32 | float64](input []D, dimensions int, batchRows int) [][]float32 {
	chunkData := make([][]float32, batchRows)
	for i := range chunkData {
		chunkData[i] = make([]float32, dimensions)
		for j := 0; j < dimensions; j++ {
			chunkData[i][j] = float32(input[i*dimensions+j])
		}
	}
	return chunkData
}

func convert1DChunk_int[D int](input []D, dimensions int, batchRows int) [][]int {
	chunkData := make([][]int, batchRows)
	for i := range chunkData {
		chunkData[i] = make([]int, dimensions)
		for j := 0; j < dimensions; j++ {
			chunkData[i][j] = int(input[i*dimensions+j])
		}
	}
	return chunkData
}

type BenchResult struct {
	BuildTime   float64
	BuildQPS    float64
	QueryTime   float64
	QueryQPS    float64
	QueryRecall float64
}

func TestBench(t *testing.T) {

	// file, err := hdf5.OpenFile("/home/ajit/datasets/sift-128-euclidean.hdf5", hdf5.F_ACC_RDONLY)
	file, err := hdf5.OpenFile("/Users/ajit/datasets/dbpedia-openai-1000k-angular.hdf5", hdf5.F_ACC_RDONLY)
	// file, err := hdf5.OpenFile("/home/ajit/datasets/fashion-mnist-784-euclidean.hdf5", hdf5.F_ACC_RDONLY)
	if err != nil {
		t.Fatalf("Error opening file: %v\n", err)
	}
	defer file.Close()

	dataset, err := file.OpenDataset("train")
	testdataset, err := file.OpenDataset("test")
	neighborsdataset, err := file.OpenDataset("neighbors")
	// distancesdataset, err := file.OpenDataset("distances")

	if err != nil {
		t.Fatalf("Error opening dataset: %v\n", err)
	}

	BuildTime, BuildQPS, index := LoadVectors(dataset)

	// Create a CPU profile file
	// f, err := os.Create("cpu_profile.prof")
	// if err != nil {
	// 	log.Fatal("could not create CPU profile: ", err)
	// }
	// defer f.Close()

	// Start CPU profiling
	// if err := pprof.StartCPUProfile(f); err != nil {
	// 	log.Fatal("could not start CPU profile: ", err)
	// }
	// defer pprof.StopCPUProfile()

	QueryTime, QueryQPS, QueryRecall := QueryVectors(&index, testdataset, neighborsdataset)

	result := BenchResult{BuildTime, BuildQPS, QueryTime, QueryQPS, QueryRecall}
	println(result.BuildQPS)
	println(result.QueryRecall)
	// return result

}

func LoadVectors(dataset *hdf5.Dataset) (float64, float64, DiskANNIndex) {

	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	byteSize, _ := getHDF5ByteSize(dataset)

	rows := dims[0]
	dimensions := dims[1]

	// rows = uint(990_000)
	rows = uint(1000)

	// batchSize := uint(990_000)
	batchSize := uint(1000)

	memspace, err := hdf5.CreateSimpleDataspace([]uint{batchSize, dimensions}, []uint{batchSize, dimensions})
	if err != nil {
		log.Fatalf("Error creating memspace: %v", err)
	}
	defer memspace.Close()

	start := time.Now()

	minValC, maxValC := float32(math.Inf(1)), float32(math.Inf(-1))

	batchRows := batchSize

	offset := []uint{0, 0}
	count := []uint{batchRows, dimensions}

	if err := dataspace.SelectHyperslab(offset, nil, count, nil); err != nil {
		log.Fatalf("Error selecting hyperslab: %v", err)
	}

	var chunkData [][]float32

	if byteSize == 4 {
		println("byte size 4")
		chunkData1D := make([]float32, batchRows*dimensions)

		if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
			log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, 0, rows)
			log.Fatalf("Error reading subset: %v", err)
		}

		chunkData = convert1DChunk[float32](chunkData1D, int(dimensions), int(batchRows))

	} else if byteSize == 8 {
		chunkData1D := make([]float64, batchRows*dimensions)

		if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
			log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, 0, rows)
			log.Fatalf("Error reading subset: %v", err)
		}

		chunkData = convert1DChunk[float64](chunkData1D, int(dimensions), int(batchRows))

	}

	ids := make([]uint64, batchSize)

	for j := uint(0); j < batchSize; j++ {
		ids[j] = uint64(0*batchSize + j)

	}

	index := NewDiskANNIndex(chunkData, ids)

	if err != nil {
		panic(err)
	}

	log.Printf("chunkData range: [%f, %f]", minValC, maxValC)

	elapsed := time.Since(start)
	println("elapsed time: ", elapsed.Seconds())
	println("QPS: ", float64(rows)/elapsed.Seconds())

	time := elapsed.Seconds()
	qps := float64(rows) / time

	return time, qps, index
}

func QueryVectors(index *DiskANNIndex, dataset *hdf5.Dataset, ideal_neighbors *hdf5.Dataset) (float64, float64, float64) {

	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	ideal_neighbors_dataspace := ideal_neighbors.Space()

	dims_ideal, _, _ := ideal_neighbors_dataspace.SimpleExtentDims()
	// print dims
	println(dims_ideal[0])
	println(dims_ideal[1])

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	byteSize, _ := getHDF5ByteSize(dataset)

	byteSize_ideal, _ := getHDF5ByteSize(ideal_neighbors)

	rows := dims[0]
	dimensions := dims[1]

	K := 10

	rows = uint(1_000)
	// rows = uint(5)

	// batchSize := uint(30_000)
	batchSize := uint(1_000)
	// batchSize := uint(5)

	// Handle offsetting the data for product quantization
	// i := uint(0)

	memspace, err := hdf5.CreateSimpleDataspace([]uint{batchSize, dimensions}, []uint{batchSize, dimensions})
	if err != nil {
		log.Fatalf("Error creating memspace: %v", err)
	}
	defer memspace.Close()

	memspace_ideal, err := hdf5.CreateSimpleDataspace([]uint{batchSize, uint(K)}, []uint{batchSize, uint(K)})
	if err != nil {
		log.Fatalf("Error creating memspace: %v", err)
	}
	defer memspace_ideal.Close()

	start := time.Now()

	numCorrect := 0

	for i := uint(0); i < rows; i += batchSize {

		batchRows := batchSize
		// handle final smaller batch
		if i+batchSize > rows {
			batchRows = rows - i
			memspace, err = hdf5.CreateSimpleDataspace([]uint{batchRows, dimensions}, []uint{batchRows, dimensions})
			if err != nil {
				log.Fatalf("Error creating final memspace: %v", err)
			}

			memspace_ideal, err = hdf5.CreateSimpleDataspace([]uint{batchRows, uint(K)}, []uint{batchRows, uint(K)})
			if err != nil {
				log.Fatalf("Error creating final memspace: %v", err)
			}
		}

		offset := []uint{i, 0}
		count := []uint{batchRows, dimensions}

		count_ideal := []uint{batchRows, uint(K)}

		if err := dataspace.SelectHyperslab(offset, nil, count, nil); err != nil {
			log.Fatalf("Error selecting hyperslab: %v", err)
		}

		if err := ideal_neighbors_dataspace.SelectHyperslab(offset, nil, count_ideal, nil); err != nil {
			log.Fatalf("Error selecting hyperslab: %v", err)
		}

		var chunkData [][]float32

		if byteSize == 4 {
			chunkData1D := make([]float32, batchRows*dimensions)

			if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData = convert1DChunk[float32](chunkData1D, int(dimensions), int(batchRows))

		} else if byteSize == 8 {
			chunkData1D := make([]float64, batchRows*dimensions)

			if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData = convert1DChunk[float64](chunkData1D, int(dimensions), int(batchRows))

		}

		var chunkData_ideal [][]int

		if byteSize_ideal == 4 {
			chunkData1D := make([]int, batchRows*uint(K))

			if err := ideal_neighbors.ReadSubset(&chunkData1D, memspace_ideal, ideal_neighbors_dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData_ideal = convert1DChunk_int[int](chunkData1D, int(K), int(batchRows))

		} else if byteSize_ideal == 8 {
			chunkData1D := make([]int, batchRows*uint(K))

			if err := ideal_neighbors.ReadSubset(&chunkData1D, memspace_ideal, ideal_neighbors_dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData_ideal = convert1DChunk_int[int](chunkData1D, int(K), int(batchRows))

		}

		// for i := range chunkData {
		// 	for j := range chunkData[i] {
		// 		println(chunkData[i][j])
		// 	}
		// }

		// for i := range chunkData {

		ids := make([]uint64, batchSize)

		for j := uint(0); j < batchSize; j++ {
			ids[j] = uint64(i*batchSize + j)

		}

		for k := range chunkData {
			ids_result := index.Search(chunkData[k], K)
			ids_ideal := chunkData_ideal[k]

			for j := range ids_result {
				// Convert the ID back to an index for comparison
				resultIndex := ids_result[j] // This is already the index since you assigned IDs sequentially
				println(resultIndex)
				println(ids_ideal[j])
				if resultIndex == uint64(ids_ideal[j]) {
					numCorrect += 1
				}
			}
		}

		// _, _, err := index.SearchByVectorBatch(chunkData, K, nil)

		// if err != nil {
		// 	panic(err)
		// }

		// }

	}

	// println("r: ", r)

	println(numCorrect)

	elapsed := time.Since(start)
	println("elapsed time (query): ", elapsed.Seconds())
	println(float64(rows))
	println("recall: ", float64(numCorrect)/float64(rows*uint(K)))
	println("QPS: ", float64(rows)/elapsed.Seconds())

	time := float64(elapsed.Seconds())

	recall := float64(numCorrect) / float64(rows*uint(K))
	qps := float64(rows) / time

	return recall, time, qps
}
