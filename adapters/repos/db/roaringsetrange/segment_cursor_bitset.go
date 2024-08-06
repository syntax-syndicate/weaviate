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
)

// A SegmentCursor iterates over all key-value pairs in a single disk segment.
// You can start at the beginning using [*SegmentCursor.First] and move forward
// using [*SegmentCursor.Next]
type SegmentCursorBS struct {
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
func NewSegmentCursorBS(data []byte) *SegmentCursorBS {
	return &SegmentCursorBS{data: data, nextOffset: 0}
}

func (c *SegmentCursorBS) First() (uint8, BitSetLayer, bool) {
	c.nextOffset = 0
	return c.Next()
}

func (c *SegmentCursorBS) Next() (uint8, BitSetLayer, bool) {
	if c.nextOffset >= uint64(len(c.data)) {
		return 0, BitSetLayer{}, false
	}

	sn := NewSegmentNodeBSFromBuffer(c.data[c.nextOffset:])
	c.nextOffset += sn.Len()

	return sn.Key(), BitSetLayer{
		Additions: sn.Additions(),
		Deletions: sn.Deletions(),
	}, true
}

type GaplessSegmentCursorBS struct {
	cursor InnerCursorBS

	started bool
	key     uint8
	lastKey uint8
	lastVal BitSetLayer
	lastOk  bool
}

func NewGaplessSegmentCursorBS(cursor InnerCursorBS) *GaplessSegmentCursorBS {
	return &GaplessSegmentCursorBS{cursor: cursor, started: false, key: 0}
}

func (c *GaplessSegmentCursorBS) First() (uint8, BitSetLayer, bool) {
	c.started = true

	c.lastKey, c.lastVal, c.lastOk = c.cursor.First()

	c.key = 1
	if c.lastOk && c.lastKey == 0 {
		return c.lastKey, c.lastVal, c.lastOk
	}
	return 0, BitSetLayer{}, true
}

func (c *GaplessSegmentCursorBS) Next() (uint8, BitSetLayer, bool) {
	if !c.started {
		return c.First()
	}

	if c.key >= 65 {
		return 0, BitSetLayer{}, false
	}

	for c.lastOk && c.lastKey < c.key {
		c.lastKey, c.lastVal, c.lastOk = c.cursor.Next()
	}

	currKey := c.key
	c.key++
	if c.lastOk && c.lastKey == currKey {
		return currKey, c.lastVal, true
	}
	return currKey, BitSetLayer{}, true
}

type SegmentCursorBSReader struct {
	readSeeker io.ReadSeeker
	reader     *bufio.Reader
	lenBuf     []byte
	dataBuf    []byte
}

// NewSegmentCursor creates a cursor for a single disk segment. Make sure that
// the data buf is already sliced correctly to start at the payload, as calling
// [*SegmentCursor.First] will start reading at offset 0 relative to the passed
// in buffer. Similarly, the buffer may only contain payloads, as the buffer end
// is used to determine if more keys can be found.
//
// Therefore if the payload is part of a longer continuous buffer, the cursor
// should be initialized with data[payloadStartPos:payloadEndPos]
func NewSegmentCursorBSReader(readSeeker io.ReadSeeker) *SegmentCursorBSReader {
	return &SegmentCursorBSReader{
		readSeeker: readSeeker,
		reader:     bufio.NewReaderSize(readSeeker, 1024*1024),
		lenBuf:     make([]byte, 8),
		dataBuf:    make([]byte, 10*1024),
	}
}

func (c *SegmentCursorBSReader) First() (uint8, BitSetLayer, bool) {
	c.readSeeker.Seek(0, io.SeekStart)
	c.reader.Reset(c.readSeeker)
	return c.Next()
}

func (c *SegmentCursorBSReader) Next() (uint8, BitSetLayer, bool) {
	// TODO pool
	n, err := io.ReadFull(c.reader, c.lenBuf)

	if err == io.EOF {
		return 0, BitSetLayer{}, false
	}

	// TODO
	if err != nil {
		panic(fmt.Sprintf("SegmentCursorBSReader::Next: %s", err.Error()))
	}
	if n != 8 {
		panic(fmt.Sprintf("SegmentCursorBSReader::Next: invalid bytes read [%d] instead [%d]", n, 8))
	}

	// TODO pool
	nodeLen := binary.LittleEndian.Uint64(c.lenBuf)
	if uint64(cap(c.dataBuf)) < nodeLen {
		c.dataBuf = make([]byte, nodeLen)
	} else {
		c.dataBuf = c.dataBuf[:nodeLen]
	}

	copy(c.dataBuf, c.lenBuf)

	// TODO
	n2, err2 := io.ReadFull(c.reader, c.dataBuf[8:])
	if err2 != nil {
		panic(fmt.Sprintf("SegmentCursorBSReader::Next2: %s", err2.Error()))
	}
	if uint64(n2) != nodeLen-8 {
		panic(fmt.Sprintf("SegmentCursorBSReader::Next2: invalid bytes read [%d] instead [%d]", n2, nodeLen-8))
	}

	sn := NewSegmentNodeBSFromBuffer(c.dataBuf)
	// c.nextOffset += sn.Len()

	return sn.Key(), BitSetLayer{
		Additions: sn.Additions(),
		Deletions: sn.Deletions(),
	}, true
}
