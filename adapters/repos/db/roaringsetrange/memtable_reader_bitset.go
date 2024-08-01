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
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/filters"
)

type MemtableReaderBS struct {
	memtable *Memtable
}

func NewMemtableReaderBS(memtable *Memtable) *MemtableReaderBS {
	return &MemtableReaderBS{memtable: memtable}
}

func (r *MemtableReaderBS) Read(ctx context.Context, value uint64, operator filters.Operator,
) (BitSetLayer, error) {
	if err := ctx.Err(); err != nil {
		return BitSetLayer{}, err
	}

	switch operator {
	case filters.OperatorEqual:
		return r.read(func(k uint64) bool { return k == value }), nil

	case filters.OperatorNotEqual:
		return r.read(func(k uint64) bool { return k != value }), nil

	case filters.OperatorLessThan:
		return r.read(func(k uint64) bool { return k < value }), nil

	case filters.OperatorLessThanEqual:
		return r.read(func(k uint64) bool { return k <= value }), nil

	case filters.OperatorGreaterThan:
		return r.read(func(k uint64) bool { return k > value }), nil

	case filters.OperatorGreaterThanEqual:
		return r.read(func(k uint64) bool { return k >= value }), nil

	default:
		return BitSetLayer{}, fmt.Errorf("operator %v not supported for segments of strategy %q",
			operator.Name(), "roaringsetrange") // TODO move strategies to separate package?
	}
}

func (r *MemtableReaderBS) read(predicate func(k uint64) bool) BitSetLayer {
	additions := NewDefaultBitSet()
	for v, k := range r.memtable.additions {
		if predicate(k) {
			additions.Set(v)
		}
	}
	deletions := NewDefaultBitSet()
	for v := range r.memtable.deletions {
		deletions.Set(v)
	}

	return BitSetLayer{
		Additions: additions,
		Deletions: deletions,
	}
}
