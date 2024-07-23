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
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// A SegmentCursor iterates over all key-value pairs in a single disk segment.
// You can start at the beginning using [*SegmentCursor.First] and move forward
// using [*SegmentCursor.Next]
type SegmentCursor struct {
	data       []byte
	nextOffset uint64
}

// NewSegmentCursor creates a cursor for a single disk segment. Make sure that
// the data buf is already sliced correctly to start at the payload, as calling
// [*SegmentCursor.First] will start reading at offset 0 relative to the passed
// in buffer. Similarly, the buffer may only contain payloads, as the buffer end
// is used to determine if more keys can be found.
//
// Therefore if the payload is part of a longer continuous buffer, the cursor
// should be initialized with data[payloadStartPos:payloadEndPos]
func NewSegmentCursor(data []byte) *SegmentCursor {
	return &SegmentCursor{data: data, nextOffset: 0}
}

func (c *SegmentCursor) First() (uint8, roaringset.BitmapLayer, bool) {
	c.nextOffset = 0
	return c.Next()
}

func (c *SegmentCursor) Next() (uint8, roaringset.BitmapLayer, bool) {
	if c.nextOffset >= uint64(len(c.data)) {
		return 0, roaringset.BitmapLayer{}, false
	}

	sn := NewSegmentNodeFromBuffer(c.data[c.nextOffset:])
	c.nextOffset += sn.Len()

	return sn.Key(), roaringset.BitmapLayer{
		Additions: sn.Additions(),
		Deletions: sn.Deletions(),
	}, true
}

type GaplessSegmentCursor struct {
	cursor *SegmentCursor

	started bool
	key     uint8
	lastKey uint8
	lastVal roaringset.BitmapLayer
	lastOk  bool
}

func NewGaplessSegmentCursor(cursor *SegmentCursor) *GaplessSegmentCursor {
	return &GaplessSegmentCursor{cursor: cursor, started: false, key: 0}
}

func (c *GaplessSegmentCursor) First() (uint8, roaringset.BitmapLayer, bool) {
	c.started = true

	c.lastKey, c.lastVal, c.lastOk = c.cursor.First()

	c.key = 1
	if c.lastOk && c.lastKey == 0 {
		return c.lastKey, c.lastVal, c.lastOk
	}
	return 0, roaringset.BitmapLayer{}, true
}

func (c *GaplessSegmentCursor) Next() (uint8, roaringset.BitmapLayer, bool) {
	if !c.started {
		return c.First()
	}

	if c.key >= 65 {
		return 0, roaringset.BitmapLayer{}, false
	}

	for c.lastOk && c.lastKey < c.key {
		c.lastKey, c.lastVal, c.lastOk = c.cursor.Next()
	}

	currKey := c.key
	c.key++
	if c.lastOk && c.lastKey == currKey {
		return currKey, c.lastVal, true
	}
	return currKey, roaringset.BitmapLayer{}, true
}
