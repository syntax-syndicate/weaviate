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

type BitSet struct {
	set  []uint8
	mask []uint8
}

func NewBitSet(size int) *BitSet {
	return &BitSet{
		set: make([]uint8, size/8+1),
		mask: []uint8{
			1, 2, 4, 8, 16, 32, 64, 128,
		},
	}
}

// Visit sets element at node to the marker value
func (bs *BitSet) Visit(node uint64) {
	bs.set[node/8] |= bs.mask[node%8]
}

// Visited checks if l contains the specified node
func (bs *BitSet) Visited(node uint64) bool {
	return int(node) < bs.Len() && (bs.set[node/8]&bs.mask[node%8] > 0)
}

func (bs *BitSet) Len() int {
	return len(bs.set) * 8
}
