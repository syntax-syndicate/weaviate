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

package lsmkv

import (
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
)

type CursorRoaringSetRangeBS interface {
	First() (uint8, roaringsetrange.BitSet, bool)
	Next() (uint8, roaringsetrange.BitSet, bool)
	Close()
}

type cursorRoaringSetRangeBS struct {
	combinedCursor *roaringsetrange.CombinedCursorBS
	unlock         func()
}

func (c *cursorRoaringSetRangeBS) First() (uint8, roaringsetrange.BitSet, bool) {
	return c.combinedCursor.First()
}

func (c *cursorRoaringSetRangeBS) Next() (uint8, roaringsetrange.BitSet, bool) {
	return c.combinedCursor.Next()
}

func (c *cursorRoaringSetRangeBS) Close() {
	c.combinedCursor.Close()
	c.unlock()
}

func (b *Bucket) CursorRoaringSetRangeBS() CursorRoaringSetRangeBS {
	MustBeExpectedStrategy(b.strategy, StrategyRoaringSetRange)

	b.flushLock.RLock()

	innerCursors, unlockSegmentGroup := b.disk.newRoaringSetRangeCursorsBS()

	// we have a flush-RLock, so we have the guarantee that the flushing state
	// will not change for the lifetime of the cursor, thus there can only be two
	// states: either a flushing memtable currently exists - or it doesn't
	if b.flushing != nil {
		innerCursors = append(innerCursors, b.flushing.newRoaringSetRangeCursorBS())
	}
	innerCursors = append(innerCursors, b.active.newRoaringSetRangeCursorBS())

	// cursors are in order from oldest to newest, with the memtable cursor
	// being at the very top
	return &cursorRoaringSetRangeBS{
		combinedCursor: roaringsetrange.NewCombinedCursorBS(innerCursors, b.logger),
		unlock: func() {
			unlockSegmentGroup()
			b.flushLock.RUnlock()
		},
	}
}
