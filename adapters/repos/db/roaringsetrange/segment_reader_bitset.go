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
	"math"

	"github.com/weaviate/weaviate/entities/filters"
)

type SegmentReaderBS struct {
	cursor *GaplessSegmentCursorBS
}

func NewSegmentReaderBS(cursor *GaplessSegmentCursorBS) *SegmentReaderBS {
	return &SegmentReaderBS{cursor: cursor}
}

func (r *SegmentReaderBS) Read(ctx context.Context, value uint64, operator filters.Operator,
) (BitSetLayer, error) {
	if err := ctx.Err(); err != nil {
		return BitSetLayer{}, err
	}

	switch operator {
	case filters.OperatorEqual:
		return r.readEqual(ctx, value)

	case filters.OperatorNotEqual:
		return r.readNotEqual(ctx, value)

	case filters.OperatorLessThan:
		return r.readLessThan(ctx, value)

	case filters.OperatorLessThanEqual:
		return r.readLessThanEqual(ctx, value)

	case filters.OperatorGreaterThan:
		return r.readGreaterThan(ctx, value)

	case filters.OperatorGreaterThanEqual:
		return r.readGreaterThanEqual(ctx, value)

	default:
		return BitSetLayer{}, fmt.Errorf("operator %v not supported for segments of strategy %q",
			operator.Name(), "roaringsetrange") // TODO move strategies to separate package?
	}
}

func (r *SegmentReaderBS) firstLayer() (BitSetLayer, bool) {
	_, layer, ok := r.cursor.First()
	if !ok {
		return BitSetLayer{
			Additions: NewDefaultBitSet(),
			Deletions: NewDefaultBitSet(),
		}, false
	}

	var deletions BitSet
	if layer.Deletions == nil {
		deletions = NewDefaultBitSet()
	} else {
		deletions = layer.Deletions.Clone()
	}

	if layer.Additions.IsEmpty() {
		return BitSetLayer{
			Additions: NewDefaultBitSet(),
			Deletions: deletions,
		}, false
	}
	// additions will be cloned when needed by merging methods
	return BitSetLayer{
		Additions: layer.Additions,
		Deletions: deletions,
	}, true
}

func (r *SegmentReaderBS) readEqual(ctx context.Context, value uint64,
) (BitSetLayer, error) {
	if value == 0 {
		return r.readLessThanEqual(ctx, value)
	}
	if value == math.MaxUint64 {
		return r.readGreaterThanEqual(ctx, value)
	}

	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	eq, err := r.mergeBetween(ctx, value, value+1, firstLayer.Additions)
	if err != nil {
		return BitSetLayer{}, err
	}

	return BitSetLayer{
		Additions: eq,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReaderBS) readNotEqual(ctx context.Context, value uint64,
) (BitSetLayer, error) {
	if value == 0 {
		return r.readGreaterThan(ctx, value)
	}
	if value == math.MaxUint64 {
		return r.readLessThan(ctx, value)
	}

	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	neq := firstLayer.Additions.Clone()
	eq, err := r.mergeBetween(ctx, value, value+1, firstLayer.Additions)
	if err != nil {
		return BitSetLayer{}, err
	}

	neq.AndNot(eq)
	return BitSetLayer{
		Additions: neq,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReaderBS) readLessThan(ctx context.Context, value uint64,
) (BitSetLayer, error) {
	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	if value == 0 {
		// no value is < 0
		return BitSetLayer{
			Additions: NewDefaultBitSet(),
			Deletions: firstLayer.Deletions,
		}, nil
	}

	lt := firstLayer.Additions.Clone()
	gte, err := r.mergeGreaterThanEqual(ctx, value, firstLayer.Additions)
	if err != nil {
		return BitSetLayer{}, err
	}

	lt.AndNot(gte)
	return BitSetLayer{
		Additions: lt,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReaderBS) readLessThanEqual(ctx context.Context, value uint64,
) (BitSetLayer, error) {
	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	if value == math.MaxUint64 {
		// all values are <= max uint64
		return firstLayer, nil
	}

	lte := firstLayer.Additions.Clone()
	gte1, err := r.mergeGreaterThanEqual(ctx, value+1, firstLayer.Additions)
	if err != nil {
		return BitSetLayer{}, err
	}

	lte.AndNot(gte1)
	return BitSetLayer{
		Additions: lte,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReaderBS) readGreaterThan(ctx context.Context, value uint64,
) (BitSetLayer, error) {
	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	if value == math.MaxUint64 {
		// no value is > max uint64
		return BitSetLayer{
			Additions: NewDefaultBitSet(),
			Deletions: firstLayer.Deletions,
		}, nil
	}

	gte1, err := r.mergeGreaterThanEqual(ctx, value+1, firstLayer.Additions)
	if err != nil {
		return BitSetLayer{}, err
	}

	return BitSetLayer{
		Additions: gte1,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReaderBS) readGreaterThanEqual(ctx context.Context, value uint64,
) (BitSetLayer, error) {
	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	gte, err := r.mergeGreaterThanEqual(ctx, value, firstLayer.Additions)
	if err != nil {
		return BitSetLayer{}, err
	}

	return BitSetLayer{
		Additions: gte,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReaderBS) mergeGreaterThanEqual(ctx context.Context, value uint64,
	all BitSet,
) (BitSet, error) {
	ANDed := false
	result := all

	for bit, layer, ok := r.cursor.Next(); ok; bit, layer, ok = r.cursor.Next() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if value&(1<<(bit-1)) != 0 {
			if !ANDed {
				result = result.Clone()
			}
			ANDed = true
			result.And(layer.Additions)
		} else if ANDed {
			result.Or(layer.Additions)
		}
	}

	if !ANDed {
		result = result.Clone()
	}

	return result, nil
}

func (r *SegmentReaderBS) mergeBetween(ctx context.Context, valueMinInc, valueMaxExc uint64,
	all BitSet,
) (BitSet, error) {
	ANDedMin := false
	ANDedMax := false
	resultMin := all
	resultMax := all

	for bit, layer, ok := r.cursor.Next(); ok; bit, layer, ok = r.cursor.Next() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		var b uint64 = 1 << (bit - 1)

		if valueMinInc&b != 0 {
			if !ANDedMin {
				resultMin = resultMin.Clone()
			}
			ANDedMin = true
			resultMin.And(layer.Additions)
		} else if ANDedMin {
			resultMin.Or(layer.Additions)
		}

		if valueMaxExc&b != 0 {
			if !ANDedMax {
				resultMax = resultMax.Clone()
			}
			ANDedMax = true
			resultMax.And(layer.Additions)
		} else if ANDedMax {
			resultMax.Or(layer.Additions)
		}
	}

	if !ANDedMin {
		resultMin = resultMin.Clone()
	}
	resultMin.AndNot(resultMax)

	return resultMin, nil
}
