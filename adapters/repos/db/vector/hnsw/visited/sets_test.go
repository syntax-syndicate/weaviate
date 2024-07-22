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

package visited

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

func BenchmarkVisitedSets(b *testing.B) {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	size := 100_000_000
	ls := NewList(size)
	for x := 0; x < size; x++ {
		if rand.Float32() < 0.05 {
			ls.Visit(uint64(x))
		}
	}
	runtime.ReadMemStats(&m2)
	fmt.Println("total:", m2.TotalAlloc-m1.TotalAlloc)
	b.ResetTimer()
	for x := 0; x < b.N; x++ {
		ls.Visited(uint64(x))
	}

}

func BenchmarkAllowList(b *testing.B) {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	size := 100_000_000
	al := helpers.NewAllowList()
	for x := 0; x < size; x++ {
		if rand.Float32() < 0.05 {
			al.Insert(uint64(x))
		}
	}
	runtime.ReadMemStats(&m2)
	fmt.Println("total:", m2.TotalAlloc-m1.TotalAlloc)
	b.ResetTimer()
	for x := 0; x < b.N; x++ {
		al.Contains(uint64(x))
	}
}

func BenchmarkBitSets(b *testing.B) {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	size := 100_000_000
	ls := NewBitSet(size)
	for x := 0; x < size; x++ {
		if rand.Float32() < 0.05 {
			ls.Visit(uint64(x))
		}
	}
	runtime.ReadMemStats(&m2)
	fmt.Println("total:", m2.TotalAlloc-m1.TotalAlloc)
	b.ResetTimer()
	for x := 0; x < b.N; x++ {
		ls.Visited(uint64(x))
	}
}
