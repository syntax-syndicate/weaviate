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
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/errors"
)

type CombinedCursorBS struct {
	cursors []InnerCursorBS
	logger  logrus.FieldLogger

	inited     bool
	states     []innerCursorStateBS
	deletions  []BitSet
	nextKey    uint8
	nextLayers BitSetLayers
	nextCh     chan struct{}
	doneCh     chan struct{}
	closeCh    chan struct{}
}

type InnerCursorBS interface {
	First() (uint8, BitSetLayer, bool)
	Next() (uint8, BitSetLayer, bool)
}

type innerCursorStateBS struct {
	key       uint8
	additions BitSet
	ok        bool
}

func NewCombinedCursorBS(innerCursors []InnerCursorBS, logger logrus.FieldLogger) *CombinedCursorBS {
	c := &CombinedCursorBS{
		cursors: innerCursors,
		logger:  logger,

		inited:     false,
		states:     make([]innerCursorStateBS, len(innerCursors)),
		deletions:  make([]BitSet, len(innerCursors)),
		nextKey:    0,
		nextLayers: make(BitSetLayers, len(innerCursors)),
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

func (c *CombinedCursorBS) First() (uint8, BitSet, bool) {
	c.inited = true
	for id, cursor := range c.cursors {
		key, layer, ok := cursor.First()
		if ok && key == 0 {
			c.deletions[id] = layer.Deletions
		} else {
			c.deletions[id] = NewDefaultBitSet()
		}

		c.states[id] = innerCursorStateBS{
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

func (c *CombinedCursorBS) Next() (uint8, BitSet, bool) {
	if !c.inited {
		return c.First()
	}

	return c.next()
}

func (c *CombinedCursorBS) Close() {
	c.closeCh <- struct{}{}
}

func (c *CombinedCursorBS) next() (uint8, BitSet, bool) {
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

func (c *CombinedCursorBS) createNext() (uint8, BitSetLayers) {
	key, ids := c.getCursorIdsWithLowestKey()
	if len(ids) == 0 {
		return 0, nil
	}

	layers := make(BitSetLayers, len(c.cursors))
	empty := NewDefaultBitSet()

	for id := range c.cursors {
		additions := empty
		deletions := c.deletions[id]

		if _, ok := ids[id]; ok {
			additions = c.states[id].additions

			// move forward used cursors
			key, layer, ok := c.cursors[id].Next()
			c.states[id] = innerCursorStateBS{
				key:       key,
				additions: layer.Additions,
				ok:        ok,
			}
		}

		// 1st addition and non 1st deletion bitmaps are mutated while flattening
		// therefore they are cloned upfront in goroutine
		if id == 0 {
			additions = additions.Clone()
		}

		layers[id] = BitSetLayer{
			Additions: additions,
			Deletions: deletions,
		}
	}

	return key, layers
}

func (c *CombinedCursorBS) getCursorIdsWithLowestKey() (uint8, map[int]struct{}) {
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
