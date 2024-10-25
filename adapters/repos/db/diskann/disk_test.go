package diskann

import (
	"fmt"
	"testing"
)

func TestReadWriteSegments(t *testing.T) {

	segment1 := VamanaSegment{
		id:        0,
		vector:    []float32{1, 2, 3},
		neighbors: []*VamanaSegment{},
	}

	segment2 := VamanaSegment{
		id:        1,
		vector:    []float32{4, 5, 6},
		neighbors: []*VamanaSegment{},
	}

	segment3 := VamanaSegment{
		id:        2,
		vector:    []float32{7, 8, 9},
		neighbors: []*VamanaSegment{},
	}

	segments := []*VamanaSegment{&segment1, &segment2, &segment3}

	chunkSize := len(segments[0].vector)*64 + 1024

	mf, err := NewMappedFile("test.bin", int64(chunkSize*len(segments)))
	if err != nil {
		panic(err)
	}

	err = WriteSegmentsToDisk(segments, int64(chunkSize), mf, int64(len(segments[0].vector)*4), int64(len(segments[0].neighbors)*8))
	if err != nil {
		panic(err)
	}

	for _, segment := range segments {
		disksegment, err := ReadSegmentFromDisk(segment.id, int64(chunkSize), mf, int64(len(segments[0].vector)*4), int64(len(segments[0].neighbors)*8))

		fmt.Printf("%+v\n", disksegment)

		if err != nil {
			t.Fatalf("Error reading segment %d: %v\n", segment.id, err)
		}
		if disksegment.Id != segment.id {
			t.Fatalf("Error reading segment %d: expected %d, got %d\n", segment.id, segment.id, disksegment.Id)
		}

		for i, seg := range disksegment.Neighbors {
			println(seg)
			println(segment.neighbors[i].id)
			if seg != segment.neighbors[i].id {
				t.Fatalf("Error reading segment %d: expected neighbor %d to be %d, got %d\n", segment.id, i, segment.neighbors[i].id, seg)
			}
		}

		for i, seg := range disksegment.Vector {
			println(seg)
			println(segment.vector[i])
			if seg != segment.vector[i] {
				t.Fatalf("Error reading segment %d: expected vector %d to be %f, got %f\n", segment.id, i, segment.vector[i], seg)
			}
		}

	}

}
