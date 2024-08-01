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
	"github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
)

type InnerReaderBS interface {
	Read(ctx context.Context, value uint64, operator filters.Operator) (BitSetLayer, error)
}

type CombinedReaderBS struct {
	readers        []InnerReaderBS
	logger         logrus.FieldLogger
	concurrency    int
	releaseReaders func()
}

func NewCombinedReaderBS(readers []InnerReaderBS, logger logrus.FieldLogger, concurrency int,
	releaseReaders func(),
) *CombinedReaderBS {
	if concurrency < 0 {
		concurrency = 0
	}

	return &CombinedReaderBS{
		readers:        readers,
		logger:         logger,
		concurrency:    concurrency,
		releaseReaders: releaseReaders,
	}
}

func (r *CombinedReaderBS) Read(ctx context.Context, value uint64, operator filters.Operator,
) (BitSet, error) {
	count := len(r.readers)

	switch count {
	case 0:
		return NewDefaultBitSet(), nil
	case 1:
		layer, err := r.readers[0].Read(ctx, value, operator)
		if err != nil {
			return nil, err
		}
		return layer.Additions, nil
	}

	// all readers but last one. it will be processed by current goroutine
	responseChans := make([]chan *readerRespBS, count-1)
	for i := range responseChans {
		responseChans[i] = make(chan *readerRespBS, 1)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	eg, gctx := errors.NewErrorGroupWithContextWrapper(r.logger, ctx)
	eg.SetLimit(r.concurrency)
	// start from the oldest ones (biggest)
	for i := 0; i < count-1; i++ {
		i := i
		eg.Go(func() error {
			s := time.Now()
			fmt.Printf(" ==> [%d] started reading\n", i)

			layer, err := r.readers[i].Read(gctx, value, operator)
			responseChans[i] <- &readerRespBS{layer, err}

			fmt.Printf(" ==> [%d] finished reading, took [%s]\n", i, time.Since(s))

			return err
		})
	}

	s := time.Now()
	fmt.Printf(" ==> [%d] started reading\n", count-1)

	layer, err := r.readers[count-1].Read(ctx, value, operator)
	if err != nil {
		return nil, err
	}

	fmt.Printf(" ==> [%d] finished reading, took [%s]\n", count-1, time.Since(s))

	// start from the newest ones
	for i := count - 2; i >= 0; i-- {
		response := <-responseChans[i]
		if response.err != nil {
			return nil, response.err
		}

		s := time.Now()
		fmt.Printf(" ==> [%d/%d] started merging\n", i+1, i)

		response.layer.Additions.AndNot(layer.Deletions)
		response.layer.Additions.Or(layer.Additions)
		response.layer.Deletions.Or(layer.Deletions)
		layer = response.layer

		fmt.Printf(" ==> [%d/%d] finished merging, took [%s]\n", i+1, i, time.Since(s))
	}

	return layer.Additions, nil
}

func (r *CombinedReaderBS) Close() {
	r.releaseReaders()
}

type readerRespBS struct {
	layer BitSetLayer
	err   error
}
