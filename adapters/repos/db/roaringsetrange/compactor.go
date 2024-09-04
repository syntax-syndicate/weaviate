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
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// Compactor takes in a left and a right segment and merges them into a single
// segment. The input segments are represented by cursors without their
// respective segmentindexes. A new segmentindex is built from the merged nodes
// without taking the old indexes into account at all.
//
// The left segment must precede the right one in its creation time, as the
// compactor applies latest-takes-presence rules when there is a conflict.
//
// # Merging independent key/value pairs
//
// The new segment's nodes will be in sorted fashion (this is a requirement for
// the segment index and segment cursors to function). To achieve a sorted end
// result, the Compactor goes over both input cursors simultaneously and always
// works on the smaller of the two keys. After a key/value pair has been added
// to the output only the input cursor that provided the pair is advanced.
//
// # Merging key/value pairs with identical keys
//
// When both segment have a key/value pair with an overlapping key, the value
// has to be merged. The merge logic is not part of the compactor itself.
// Instead it makes use of [BitmapLayers.Merge].
//
// # Exit Criterium
//
// When both cursors no longer return values, all key/value pairs are
// considered compacted. The compactor then deals with metadata.
//
// # Index and Header metadata
//
// Once the key/value pairs have been compacted, the input writer is rewinded
// to be able to write the header metadata at the beginning of the file
// Because of this, the input writer must be an [io.WriteSeeker],
// such as [*os.File].
//
// The level of the resulting segment is the input level increased by one.
// Levels help the "eligible for compaction" cycle to find suitable compaction
// pairs.
type Compactor struct {
	left, right  SegmentCursor
	currentLevel uint16
	// Tells if deletions or keys without corresponding values
	// can be removed from merged segment.
	// (left segment is root (1st) one, keepTombstones is off for bucket)
	cleanupDeletions bool

	w    io.WriteSeeker
	bufw *bufio.Writer
}

// NewCompactor from left (older) and right (newer) seeker. See [Compactor] for
// an explanation of what goes on under the hood, and why the input
// requirements are the way they are.
func NewCompactor(w io.WriteSeeker, left, right SegmentCursor,
	level uint16, cleanupDeletions bool,
) *Compactor {
	return &Compactor{
		left:             left,
		right:            right,
		w:                w,
		bufw:             bufio.NewWriterSize(w, 256*1024),
		currentLevel:     level,
		cleanupDeletions: cleanupDeletions,
	}
}

// Do starts a compaction. See [Compactor] for an explanation of this process.
func (c *Compactor) Do() error {
	if err := c.init(); err != nil {
		return fmt.Errorf("init: %w", err)
	}

	written, err := c.writeNodes()
	if err != nil {
		return fmt.Errorf("write keys: %w", err)
	}

	// flush buffered, so we can safely seek on underlying writer
	if err := c.bufw.Flush(); err != nil {
		return fmt.Errorf("flush buffered: %w", err)
	}

	dataEnd := segmentindex.HeaderSize + uint64(written)
	if err := c.writeHeader(dataEnd); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	return nil
}

func (c *Compactor) init() error {
	// write a dummy header, we don't know the contents of the actual header yet,
	// we will seek to the beginning and overwrite the actual header at the very
	// end

	if _, err := c.bufw.Write(make([]byte, segmentindex.HeaderSize)); err != nil {
		return errors.Wrap(err, "write empty header")
	}

	return nil
}

func (c *Compactor) writeNodes() (int, error) {
	nc := &nodeCompactor{
		left:             c.left,
		right:            c.right,
		bufw:             c.bufw,
		cleanupDeletions: c.cleanupDeletions,
		emptyBitmap:      sroar.NewBitmap(),
	}

	if err := nc.loopThroughKeys(); err != nil {
		return 0, err
	}

	return nc.written, nil
}

// writeHeader assumes that everything has been written to the underlying
// writer and it is now safe to seek to the beginning and override the initial
// header
func (c *Compactor) writeHeader(startOfIndex uint64) error {
	if _, err := c.w.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek to beginning to write header")
	}

	h := &segmentindex.Header{
		Level:            c.currentLevel,
		Version:          0,
		SecondaryIndices: 0,
		Strategy:         segmentindex.StrategyRoaringSetRange,
		IndexStart:       startOfIndex,
	}

	if _, err := h.WriteTo(c.w); err != nil {
		return err
	}

	return nil
}

// nodeCompactor is a helper type to improve the code structure of merging
// nodes in a compaction
type nodeCompactor struct {
	left, right SegmentCursor
	bufw        *bufio.Writer
	written     int

	keyLeft, keyRight             uint8
	valueLeft, valueRight         roaringset.BitmapLayer
	okLeft, okRight               bool
	deletionsLeft, deletionsRight *sroar.Bitmap

	cleanupDeletions bool
	emptyBitmap      *sroar.Bitmap
}

func (nc *nodeCompactor) loopThroughKeys() error {
	nc.keyLeft, nc.valueLeft, nc.okLeft = nc.left.First()
	if nc.okLeft && nc.keyLeft == 0 {
		nc.deletionsLeft = nc.valueLeft.Deletions
		fmt.Printf("  ==> loopThroughKeys okLeft [%v] keyLeft [%v] delLeft [%v]\n",
			nc.okLeft, nc.keyLeft, nc.deletionsLeft.ToArray())
	} else {
		nc.deletionsLeft = nc.emptyBitmap
		fmt.Printf("  ==> loopThroughKeys okLeft [%v] keyLeft [%v] emptyLeft\n",
			nc.okLeft, nc.keyLeft)
	}

	nc.keyRight, nc.valueRight, nc.okRight = nc.right.First()
	if nc.okRight && nc.keyRight == 0 {
		fmt.Printf("  ==> loopThroughKeys okRight [%v] keyRight [%v] delRight [%v]\n",
			nc.okRight, nc.keyRight, nc.deletionsRight.ToArray())
		nc.deletionsRight = nc.valueRight.Deletions
	} else {
		nc.deletionsRight = nc.emptyBitmap
		fmt.Printf("  ==> loopThroughKeys okRight [%v] keyRight [%v] emptyRight\n",
			nc.okRight, nc.keyRight)
	}

	var err error
	for {
		if !nc.okLeft && !nc.okRight {
			fmt.Printf("  ==> loopThroughKeys !nc.okLeft && !nc.okRight\n")
			return nil
		}

		if nc.okLeft && nc.okRight {
			if nc.keyLeft == nc.keyRight {
				fmt.Printf("  ==> loopThroughKeys nc.mergeIdenticalKeys\n")
				err = nc.mergeIdenticalKeys()
			} else if nc.keyLeft < nc.keyRight {
				fmt.Printf("  ==> loopThroughKeys nc.takeLeftKey\n")
				err = nc.takeLeftKey()
			} else {
				fmt.Printf("  ==> loopThroughKeys nc.takeRightKey\n")
				err = nc.takeRightKey()
			}
		} else if nc.okLeft {
			fmt.Printf("  ==> loopThroughKeys nc.takeLeftKey 2\n")
			err = nc.takeLeftKey()
		} else {
			fmt.Printf("  ==> loopThroughKeys nc.takeRightKey 2\n")
			err = nc.takeRightKey()
		}

		if err != nil {
			return err
		}
	}
}

func (nc *nodeCompactor) mergeIdenticalKeys() error {
	fmt.Printf("  ==> mergeIdenticalKeys\n")
	layers := roaringset.BitmapLayers{
		{Additions: nc.valueLeft.Additions, Deletions: nc.deletionsLeft},
		{Additions: nc.valueRight.Additions, Deletions: nc.deletionsRight},
	}

	if err := nc.mergeLayers(nc.keyLeft, layers, "merged"); err != nil {
		return err
	}

	nc.keyLeft, nc.valueLeft, nc.okLeft = nc.left.Next()
	nc.keyRight, nc.valueRight, nc.okRight = nc.right.Next()
	return nil
}

func (nc *nodeCompactor) takeLeftKey() error {
	fmt.Printf("  ==> takeLeftKey\n")
	layers := roaringset.BitmapLayers{
		{Additions: nc.valueLeft.Additions, Deletions: nc.deletionsLeft},
		{Additions: nc.emptyBitmap, Deletions: nc.deletionsRight},
	}
	if err := nc.mergeLayers(nc.keyLeft, layers, "left"); err != nil {
		return err
	}

	nc.keyLeft, nc.valueLeft, nc.okLeft = nc.left.Next()
	return nil
}

func (nc *nodeCompactor) takeRightKey() error {
	fmt.Printf("  ==> takeRightKey\n")
	layers := roaringset.BitmapLayers{
		{Additions: nc.emptyBitmap, Deletions: nc.deletionsLeft},
		{Additions: nc.valueRight.Additions, Deletions: nc.deletionsRight},
	}
	if err := nc.mergeLayers(nc.keyRight, layers, "right"); err != nil {
		return err
	}

	nc.keyRight, nc.valueRight, nc.okRight = nc.right.Next()
	return nil
}

func (nc *nodeCompactor) mergeLayers(key uint8, layers roaringset.BitmapLayers, name string) error {
	fmt.Printf("  ==> mergeLayers [%s] key [%d]\n     layer 1 add %v\n     layer 1 del %v\n     layer 2 add %v\n     layer 2 del %v\n",
		name, key,
		layers[0].Additions.ToArray(), layers[0].Deletions.ToArray(),
		layers[1].Additions.ToArray(), layers[1].Deletions.ToArray())

	if key == 0 || key == 4 {
		layers[1].Additions.Stats()
	}

	merged, err := layers.Merge2()
	if err != nil {
		return fmt.Errorf("merge bitmap layers for %s key %d: %w", name, key, err)
	}

	fmt.Printf("  ==> mergeLayers [%s] key [%d]\n     add %v\n     del %v\n",
		name, key, merged.Additions.ToArray(), merged.Deletions.ToArray())

	if additions, deletions, skip := nc.cleanupValues(merged.Additions, merged.Deletions); !skip {
		fmt.Printf("  ==> mergeLayers [%s] key [%d] cleanup\n     add %v\n     del %v\n",
			name, key, additions.ToArray(), deletions.ToArray())

		sn, err := NewSegmentNode(key, additions, deletions)
		if err != nil {
			return fmt.Errorf("new segment node for %s key %d: %w", name, key, err)
		}

		n, err := nc.bufw.Write(sn.ToBuffer())
		if err != nil {
			return fmt.Errorf("write individual node for %s key %d: %w", name, key, err)
		}

		nc.written += n
	}
	return nil
}

func (nc *nodeCompactor) cleanupValues(additions, deletions *sroar.Bitmap,
) (add, del *sroar.Bitmap, skip bool) {
	if !nc.cleanupDeletions {
		return additions, deletions, false
	}
	if !additions.IsEmpty() {
		return additions, nc.emptyBitmap, false
	}
	return nil, nil, true
}
