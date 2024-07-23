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

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
)

type SegmentReader struct {
	cursor *GaplessSegmentCursor
}

func NewSegmentReader(cursor *GaplessSegmentCursor) *SegmentReader {
	return &SegmentReader{cursor: cursor}
}

func (r *SegmentReader) Read(ctx context.Context, value uint64, operator filters.Operator,
) (roaringset.BitmapLayer, error) {
	if err := ctx.Err(); err != nil {
		return roaringset.BitmapLayer{}, err
	}

	switch operator {
	// case filters.OperatorEqual:
	// case filters.OperatorNotEqual:
	// case filters.OperatorLessThanEqual:
	// case filters.OperatorLessThan:
	// case filters.OperatorGreaterThan:
	case filters.OperatorGreaterThanEqual:
		return r.readGreaterThanEqual(ctx, value)
	default:
		return roaringset.BitmapLayer{}, fmt.Errorf("operator %v not supported for segments of strategy %q",
			operator.Name(), "roaringsetrange") // TODO move strategies to separate package?
	}
}

func (r *SegmentReader) readGreaterThanEqual(ctx context.Context, value uint64,
) (roaringset.BitmapLayer, error) {
	bit, layer, ok := r.cursor.First()
	if !ok {
		return roaringset.BitmapLayer{}, nil
	}
	if layer.Additions.IsEmpty() {
		return layer, nil
	}

	ANDed := false
	resBM := layer.Additions
	delBM := layer.Deletions

	for bit, layer, ok = r.cursor.Next(); ok; bit, layer, ok = r.cursor.Next() {
		if ctx.Err() != nil {
			return roaringset.BitmapLayer{}, ctx.Err()
		}

		if value&(1<<(bit-1)) != 0 {
			if !ANDed {
				resBM = resBM.Clone()
			}
			ANDed = true
			resBM.And(layer.Additions)
			resBM = roaringset.Condense(resBM)
		} else if ANDed {
			resBM.Or(layer.Additions)
		}
	}

	if !ANDed {
		resBM = resBM.Clone()
	}

	return roaringset.BitmapLayer{Additions: resBM, Deletions: delBM}, nil
}
