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
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

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
	cursor InnerCursor

	started bool
	key     uint8
	lastKey uint8
	lastVal roaringset.BitmapLayer
	lastOk  bool
}

func NewGaplessSegmentCursor(cursor InnerCursor) *GaplessSegmentCursor {
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

// A SegmentCursor iterates over all key-value pairs in a single disk segment.
// You can start at the beginning using [*SegmentCursor.First] and move forward
// using [*SegmentCursor.Next]
type SegmentCursorReader struct {
	offset     int64
	readSeeker io.ReadSeeker
	reader     *bufio.Reader
}

// NewSegmentCursor creates a cursor for a single disk segment. Make sure that
// the data buf is already sliced correctly to start at the payload, as calling
// [*SegmentCursor.First] will start reading at offset 0 relative to the passed
// in buffer. Similarly, the buffer may only contain payloads, as the buffer end
// is used to determine if more keys can be found.
//
// Therefore if the payload is part of a longer continuous buffer, the cursor
// should be initialized with data[payloadStartPos:payloadEndPos]
func NewSegmentCursorReader(readSeeker io.ReadSeeker, offset int64) *SegmentCursorReader {
	return &SegmentCursorReader{
		readSeeker: readSeeker,
		reader:     bufio.NewReaderSize(readSeeker, 1024*1024),
		offset:     offset,
	}
}

func (c *SegmentCursorReader) First() (uint8, roaringset.BitmapLayer, bool) {
	c.readSeeker.Seek(c.offset, io.SeekStart)
	c.reader.Reset(c.readSeeker)
	return c.Next()
}

func (c *SegmentCursorReader) Next() (uint8, roaringset.BitmapLayer, bool) {
	// TODO pool
	buf := make([]byte, 8)
	n, err := io.ReadFull(c.reader, buf)

	if err == io.EOF {
		return 0, roaringset.BitmapLayer{}, false
	}

	// TODO
	if err != nil {
		panic(fmt.Sprintf("SegmentCursorReader::Next: %s", err.Error()))
	}
	if n != 8 {
		panic(fmt.Sprintf("SegmentCursorReader::Next: invalid bytes read [%d] instead [%d]", n, 8))
	}

	// TODO pool
	nodeLen := binary.LittleEndian.Uint64(buf)
	buf2 := make([]byte, nodeLen)
	copy(buf2, buf)

	// TODO
	n2, err2 := io.ReadFull(c.reader, buf2[8:])
	if err2 != nil {
		panic(fmt.Sprintf("SegmentCursorReader::Next2: %s", err2.Error()))
	}
	if uint64(n2) != nodeLen-8 {
		panic(fmt.Sprintf("SegmentCursorReader::Next2: invalid bytes read [%d] instead [%d]", n2, nodeLen-8))
	}

	sn := NewSegmentNodeFromBuffer(buf2)
	// c.nextOffset += sn.Len()

	return sn.Key(), roaringset.BitmapLayer{
		Additions: sn.Additions(),
		Deletions: sn.Deletions(),
	}, true
}
