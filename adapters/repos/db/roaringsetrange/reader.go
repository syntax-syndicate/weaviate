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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
)

type InnerReader interface {
	Read(ctx context.Context, value uint64, operator filters.Operator) (roaringset.BitmapLayer, error)
}

type CombinedReader struct {
	readers        []InnerReader
	logger         logrus.FieldLogger
	concurrency    int
	releaseReaders func()
}

func NewCombinedReader(readers []InnerReader, logger logrus.FieldLogger, concurrency int,
	releaseReaders func(),
) *CombinedReader {
	if concurrency < 0 {
		concurrency = 0
	}

	return &CombinedReader{
		readers:        readers,
		logger:         logger,
		concurrency:    concurrency,
		releaseReaders: releaseReaders,
	}
}

func (r *CombinedReader) Read(ctx context.Context, value uint64, operator filters.Operator,
) (*sroar.Bitmap, error) {
	count := len(r.readers)

	switch count {
	case 0:
		return sroar.NewBitmap(), nil
	case 1:
		layer, err := r.readers[0].Read(ctx, value, operator)
		if err != nil {
			return nil, err
		}
		return layer.Additions, nil
	}

	// all readers but last one. it will be processed by current goroutine
	responseChans := make([]chan *readerResp, count-1)
	for i := range responseChans {
		responseChans[i] = make(chan *readerResp, 1)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	eg, gctx := errors.NewErrorGroupWithContextWrapper(r.logger, ctx)
	eg.SetLimit(r.concurrency)
	// start from the oldest ones (biggest)
	for i := 0; i < count-1; i++ {
		i := i
		eg.Go(func() error {
			layer, err := r.readers[i].Read(gctx, value, operator)
			responseChans[i] <- &readerResp{layer, err}
			return err
		})
	}

	layer, err := r.readers[count-1].Read(ctx, value, operator)
	if err != nil {
		return nil, err
	}

	// start from the newest ones
	for i := count - 2; i >= 0; i-- {
		response := <-responseChans[i]
		if response.err != nil {
			return nil, response.err
		}

		response.layer.Additions.AndNot(layer.Deletions)
		response.layer.Additions.Or(layer.Additions)
		response.layer.Deletions.Or(layer.Deletions)
		layer = response.layer
	}

	return layer.Additions, nil
}

func (r *CombinedReader) Close() {
	r.releaseReaders()
}

type readerResp struct {
	layer roaringset.BitmapLayer
	err   error
}
