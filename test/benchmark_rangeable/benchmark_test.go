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

package benchmarkrangeable

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/filters"
	"golang.org/x/sync/errgroup"
)

var (
	dirLsm   = "../../data-weaviate-0/rangeable/6AH8HhhmczJx/lsm/property_nf_rangeable"
	segments = []string{
		"segment-1719438284846555682.db",
		"segment-1719971641517426314.db",
		"segment-1720242926200850922.db",
		"segment-1720383006099163416.db",
		"segment-1720457804050882823.db",
		"segment-1720490726000453273.db",
		"segment-1720498935309534913.db",
		"segment-1720503067652593318.db",
		"segment-1720505123214410045.db",
		"segment-1720506184365023528.db",
		"segment-1720506711659850270.db",
		"segment-1720506959712399643.db",
	}
)

// go test -v -bench BenchmarkMergeSegmentsSequencePread -benchmem -run ^$ github.com/weaviate/weaviate/test/benchmark_rangeable
func BenchmarkMergeSegmentsSequencePread(b *testing.B) {
	total := time.Now()
	for i := 0; i < len(segments); i++ {
		single := time.Now()
		path := filepath.Join(dirLsm, segments[i])
		merge_segment_pread(path)
		fmt.Printf("merging segment [%s] took [%s]\n\n", path, time.Since(single))
	}
	fmt.Printf("merging all segment took [%s]\n\n", time.Since(total))
}

// go test -v -bench BenchmarkMergeSegmentsParallelPread -benchmem -run ^$ github.com/weaviate/weaviate/test/benchmark_rangeable
func BenchmarkMergeSegmentsParallelPread(b *testing.B) {
	conc := 4

	eg := new(errgroup.Group)
	eg.SetLimit(conc)

	total := time.Now()
	for i := 0; i < len(segments); i++ {
		i := i
		eg.Go(func() error {
			single := time.Now()
			path := filepath.Join(dirLsm, segments[i])
			merge_segment_pread(path)
			fmt.Printf("merging segment [%s] took [%s]\n\n", path, time.Since(single))

			return nil
		})

	}
	eg.Wait()

	fmt.Printf("merging all segment took [%s]\n\n", time.Since(total))
}

func merge_segment_pread(path string) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("open file: %s", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("stat file: %s", err)
	}

	reader := io.NewSectionReader(file, segmentindex.HeaderSize, fileInfo.Size())
	segmentCursor := roaringsetrange.NewSegmentCursorReader(reader)
	gaplessSegmentCursor := roaringsetrange.NewGaplessSegmentCursor(segmentCursor)
	segmentReader := roaringsetrange.NewSegmentReader(gaplessSegmentCursor)

	val := uint64(13891384759934908577)
	segmentReader.Read(context.Background(), val, filters.OperatorGreaterThan)
}
