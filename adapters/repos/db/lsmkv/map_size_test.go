package lsmkv

import "testing"

// var x map[uint64]uint64

//          1:          48
//         10:         336
//        100:       5_448
//      1_000:      41_032
//     10_000:     319_560
//    100_000:   5_013_578
//  1_000_000:  40_108_110
// 10_000_000: 320_864_337

func BenchmarkEmptyMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		x := make(map[uint64]uint64, 1000000)
		for j := uint64(0); j < 1_000_000; j++ {
			x[j] = j
		}
	}
}
