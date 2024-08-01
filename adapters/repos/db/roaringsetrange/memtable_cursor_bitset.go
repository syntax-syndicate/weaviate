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

package roaringsetrange

import (
	"sync"

	"github.com/weaviate/weaviate/entities/errors"
)

type MemtableCursorBS struct {
	nodes   []*MemtableNodeBS
	nextPos int
}

func NewMemtableCursorBS(memtable *Memtable) *MemtableCursorBS {
	return &MemtableCursorBS{nodes: memtable.NodesBS()}
}

func (c *MemtableCursorBS) First() (uint8, BitSetLayer, bool) {
	c.nextPos = 0
	return c.Next()
}

func (c *MemtableCursorBS) Next() (uint8, BitSetLayer, bool) {
	if c.nextPos >= len(c.nodes) {
		return 0, BitSetLayer{}, false
	}

	mn := c.nodes[c.nextPos]
	c.nextPos++

	return mn.Key, BitSetLayer{
		Additions: mn.Additions,
		Deletions: mn.Deletions,
	}, true
}

type MemtableNodeBS struct {
	Key       uint8
	Additions BitSet
	Deletions BitSet
}

func (m *Memtable) NodesBS() []*MemtableNodeBS {
	if len(m.additions) == 0 && len(m.deletions) == 0 {
		return []*MemtableNodeBS{}
	}

	nnDeletions := NewDefaultBitSet()
	nnAdditions := NewDefaultBitSet()
	var bitsAdditions [64]BitSet

	for v := range m.deletions {
		nnDeletions.Set(v)
	}
	for v := range m.additions {
		nnDeletions.Set(v)
		nnAdditions.Set(v)
	}

	routines := 8
	wg := new(sync.WaitGroup)
	wg.Add(routines - 1)

	for i := 0; i < routines-1; i++ {
		i := i
		errors.GoWrapper(func() {
			for j := 0; j < 64; j += routines {
				bit := i + j
				for value, key := range m.additions {
					if key&(1<<bit) != 0 {
						if bitsAdditions[bit] == nil {
							bitsAdditions[bit] = NewDefaultBitSet()
						}
						bitsAdditions[bit].Set(value)
					}
				}
			}
			wg.Done()
		}, m.logger)
	}

	for bit := routines - 1; bit < 64; bit += routines {
		for value, key := range m.additions {
			if key&(1<<bit) != 0 {
				if bitsAdditions[bit] == nil {
					bitsAdditions[bit] = NewDefaultBitSet()
				}
				bitsAdditions[bit].Set(value)
			}
		}
	}
	wg.Wait()

	nodes := make([]*MemtableNodeBS, 1, 65)
	nodes[0] = &MemtableNodeBS{
		Key:       0,
		Additions: nnAdditions,
		Deletions: nnDeletions,
	}

	empty := NewDefaultBitSet()
	for bit := range bitsAdditions {
		if bitsAdditions[bit] != nil {
			nodes = append(nodes, &MemtableNodeBS{
				Key:       uint8(bit) + 1,
				Additions: bitsAdditions[bit],
				Deletions: empty,
			})
		}
	}

	return nodes
}
