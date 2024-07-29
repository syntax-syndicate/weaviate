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
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/errors"
)

type CombinedCursor struct {
	cursors []InnerCursor
	logger  logrus.FieldLogger

	inited     bool
	states     []innerCursorState
	deletions  []*sroar.Bitmap
	nextKey    uint8
	nextLayers roaringset.BitmapLayers
	nextCh     chan struct{}
	doneCh     chan struct{}
	closeCh    chan struct{}
}

type InnerCursor interface {
	First() (uint8, roaringset.BitmapLayer, bool)
	Next() (uint8, roaringset.BitmapLayer, bool)
}

type innerCursorState struct {
	key       uint8
	additions *sroar.Bitmap
	ok        bool
}

func NewCombinedCursor(innerCursors []InnerCursor, logger logrus.FieldLogger) *CombinedCursor {
	c := &CombinedCursor{
		cursors: innerCursors,
		logger:  logger,

		inited:     false,
		states:     make([]innerCursorState, len(innerCursors)),
		deletions:  make([]*sroar.Bitmap, len(innerCursors)),
		nextKey:    0,
		nextLayers: make(roaringset.BitmapLayers, len(innerCursors)),
		nextCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
		closeCh:    make(chan struct{}, 1),
	}

	errors.GoWrapper(func() {
		for {
			select {
			case <-c.nextCh:
				c.nextKey, c.nextLayers = c.createNext()
				c.doneCh <- struct{}{}
			case <-c.closeCh:
				return
			}
		}
	}, logger)

	return c
}

func (c *CombinedCursor) First() (uint8, *sroar.Bitmap, bool) {
	c.inited = true
	for id, cursor := range c.cursors {
		key, layer, ok := cursor.First()
		if ok && key == 0 {
			c.deletions[id] = layer.Deletions
		} else {
			c.deletions[id] = sroar.NewBitmap()
		}

		c.states[id] = innerCursorState{
			key:       key,
			additions: layer.Additions,
			ok:        ok,
		}
	}

	// init next layers
	c.nextCh <- struct{}{}
	<-c.doneCh

	return c.next()
}

func (c *CombinedCursor) Next() (uint8, *sroar.Bitmap, bool) {
	if !c.inited {
		return c.First()
	}

	return c.next()
}

func (c *CombinedCursor) Close() {
	c.closeCh <- struct{}{}
}

func (c *CombinedCursor) next() (uint8, *sroar.Bitmap, bool) {
	if c.nextLayers == nil {
		return 0, nil, false
	}

	key := c.nextKey
	layers := c.nextLayers
	c.nextCh <- struct{}{}
	flat := layers.FlattenMutate()
	<-c.doneCh

	return key, flat, true
}

func (c *CombinedCursor) createNext() (uint8, roaringset.BitmapLayers) {
	key, ids := c.getCursorIdsWithLowestKey()
	if len(ids) == 0 {
		return 0, nil
	}

	layers := make(roaringset.BitmapLayers, len(c.cursors))
	empty := sroar.NewBitmap()

	for id := range c.cursors {
		additions := empty
		deletions := c.deletions[id]

		if _, ok := ids[id]; ok {
			additions = c.states[id].additions

			// move forward used cursors
			key, layer, ok := c.cursors[id].Next()
			c.states[id] = innerCursorState{
				key:       key,
				additions: layer.Additions,
				ok:        ok,
			}
		}

		// 1st addition and non 1st deletion bitmaps are mutated while flattening
		// therefore they are cloned upfront in goroutine
		if id == 0 {
			additions = additions.Clone()
		} else {
			deletions = deletions.Clone()
		}

		layers[id] = roaringset.BitmapLayer{
			Additions: additions,
			Deletions: deletions,
		}
	}

	return key, layers
}

func (c *CombinedCursor) getCursorIdsWithLowestKey() (uint8, map[int]struct{}) {
	var lowestKey uint8
	ids := map[int]struct{}{}

	for id, state := range c.states {
		if !state.ok {
			continue
		}

		if len(ids) == 0 {
			lowestKey = state.key
			ids[id] = struct{}{}
		} else if lowestKey == state.key {
			ids[id] = struct{}{}
		} else if lowestKey > state.key {
			lowestKey = state.key
			ids = map[int]struct{}{id: {}}
		}
	}

	return lowestKey, ids
}

type CombinedCursor2 struct {
	cursors        []InnerCursor
	logger         logrus.FieldLogger
	concurrency    int
	releaseCursors func()

	started   bool
	key       uint8
	deletions []*sroar.Bitmap
	chans     [64]chan *sroar.Bitmap
	cancelCtx context.CancelFunc
}

// assumes no gaps in cursors
func NewCombinedCursor2(cursors []InnerCursor, logger logrus.FieldLogger, concurrency int,
	releaseCursors func(),
) *CombinedCursor2 {
	return &CombinedCursor2{
		cursors:        cursors,
		logger:         logger,
		key:            0,
		started:        false,
		cancelCtx:      nil,
		deletions:      make([]*sroar.Bitmap, len(cursors)),
		concurrency:    concurrency,
		releaseCursors: releaseCursors,
	}
}

// TODO safe impl for 0 cursors
func (c *CombinedCursor2) First() (uint8, *sroar.Bitmap, bool) {
	c.started = true
	c.cancel() // cancel ongoing processing to start new one

	for i := range c.chans {
		c.chans[i] = make(chan *sroar.Bitmap, 1)
	}

	c.key = 0
	// additions := make([]*sroar.Bitmap, len(c.cursors))
	// for i := range c.cursors {
	// 	key, layer, ok := c.cursors[i].First()
	// 	if !ok || key != 0 {
	// 		panic("inner cursor failed1")
	// 	}
	// 	c.deletions[i] = layer.Deletions
	// 	additions[i] = layer.Additions
	// }

	var merged *sroar.Bitmap
	for i := range c.cursors {
		key, layer, ok := c.cursors[i].First()
		if !ok || key != 0 {
			panic("inner cursor failed1")
		}
		if i == 0 {
			merged = layer.Additions.Clone()
		} else {
			deletions := layer.Deletions.Clone()
			deletions.And(merged)

			fmt.Printf("  ==> deletions [%2d] size [%d]\n", i, len(deletions.ToBuffer()))

			merged.AndNot(deletions)
			merged.Or(layer.Additions)

			fmt.Printf("  ==> merged [%2d] size [%d]\n", i, len(deletions.ToBuffer()))

			c.deletions[i] = roaringset.Condense(deletions)
		}
	}

	context, cancel := context.WithCancel(context.Background())
	c.cancelCtx = cancel
	eg, _ := errors.NewErrorGroupWithContextWrapper(c.logger, context)
	eg.SetLimit(c.concurrency)

	eg.Go(func() error {
		for k := uint8(1); k <= 64; k++ {
			k := k
			additions := make([]*sroar.Bitmap, len(c.cursors))
			for i := range c.cursors {
				key, layer, ok := c.cursors[i].Next()
				if !ok || key != k {
					panic("inner cursor failed2")
				}
				additions[i] = layer.Additions
			}
			eg.Go(func() error {
				c.chans[k-1] <- c.mergeKey(k, additions)
				return nil
			})
		}
		return nil
	})

	return c.key, merged, true
}
func (c *CombinedCursor2) Next() (uint8, *sroar.Bitmap, bool) {
	if !c.started {
		return c.First()
	}

	if c.key >= 64 {
		return 0, nil, false
	}

	c.key++
	return c.key, <-c.chans[c.key-1], true
}

func (c *CombinedCursor2) mergeKey(k uint8, additions []*sroar.Bitmap) *sroar.Bitmap {
	s := time.Now()
	fmt.Printf("  ==> key [%2d] merge started\n", k)
	defer func() {
		fmt.Printf("  ==> key [%2d] merge finished, took %s\n", k, time.Since(s))
	}()

	if len(additions) == 0 {
		return sroar.NewBitmap()
	}

	merged := additions[0].Clone()
	for i := 1; i < len(additions); i++ {
		merged.AndNot(c.deletions[i])
		merged.Or(additions[i])
	}
	return merged
}

func (c *CombinedCursor2) Close() {
	c.cancel()
	c.releaseCursors()
}

func (c *CombinedCursor2) cancel() {
	if c.cancelCtx != nil {
		c.cancelCtx()
		c.cancelCtx = nil
	}
}
