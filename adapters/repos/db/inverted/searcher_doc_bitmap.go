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

package inverted

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
)

func (s *Searcher) docBitmap(ctx context.Context, b *lsmkv.Bucket, limit int,
	pv *propValuePair,
) (docBitmap, error) {
	// geo props cannot be served by the inverted index and they require an
	// external index. So, instead of trying to serve this chunk of the filter
	// request internally, we can pass it to an external geo index
	if pv.operator == filters.OperatorWithinGeoRange {
		return s.docBitmapGeo(ctx, pv)
	}

	start := time.Now()
	defer func() {
		fmt.Printf("  ==> strategy [%s] took [%s]\n\n", b.Strategy(), time.Since(start))
	}()
	// all other operators perform operations on the inverted index which we
	// can serve directly
	switch b.Strategy() {
	case lsmkv.StrategySetCollection:
		return s.docBitmapInvertedSet(ctx, b, limit, pv)
	case lsmkv.StrategyRoaringSet:
		return s.docBitmapInvertedRoaringSet(ctx, b, limit, pv)
	case lsmkv.StrategyRoaringSetRange:
		fmt.Printf("  ==> value [%v]\n", binary.BigEndian.Uint64(pv.value))

		// t_cursor := time.Now()
		// dbm_cursor, err_cursor := s.docBitmapInvertedRoaringSetRange_Cursor(ctx, b, pv)
		// if err_cursor == nil {
		// 	fmt.Printf("  ==> search cursor took [%s] op [%s] card [%d] size [%d]\n",
		// 		time.Since(t_cursor).String(), pv.operator.Name(),
		// 		dbm_cursor.docIDs.GetCardinality(), len(dbm_cursor.docIDs.ToBuffer()))
		// }
		// return dbm_cursor, err_cursor

		t_reader := time.Now()
		dbm_reader, err_reader := s.docBitmapInvertedRoaringSetRange_Reader(ctx, b, pv)
		if err_reader == nil {
			fmt.Printf("  ==> search reader took [%s] op [%s] card [%d] size [%d]\n",
				time.Since(t_reader).String(), pv.operator.Name(),
				dbm_reader.docIDs.GetCardinality(), len(dbm_reader.docIDs.ToBuffer()))
		}
		return dbm_reader, err_reader

		// // t3 := time.Now()
		// // dbm3, err3 := s.docBitmapInvertedRoaringSetRange3(ctx, b, pv)
		// // if err3 == nil {
		// // 	fmt.Printf("  ==> search took [%s] op [%s] card [%d] size [%d]\n",
		// // 		time.Since(t3).String(), pv.operator.Name(),
		// // 		dbm3.docIDs.GetCardinality(), len(dbm3.docIDs.ToBuffer()))
		// // }
		// // return dbm3, err3

		// // t_cursor_bs := time.Now()
		// // dbm_cursor_bs, err_cursor_bs := s.docBitmapInvertedRoaringSetRange_Cursor_BitSet(ctx, b, pv)
		// // if err_cursor_bs == nil {
		// // 	fmt.Printf("  ==> search took [%s] op [%s] card [%d] size [%d]\n",
		// // 		time.Since(t_cursor_bs).String(), pv.operator.Name(),
		// // 		dbm_cursor_bs.docIDs.GetCardinality(), len(dbm_cursor_bs.docIDs.ToBuffer()))
		// // }
		// // return dbm_cursor_bs, err_cursor_bs

		// t_reader_bs := time.Now()
		// dbm_reader_bs, err_reader_bs := s.docBitmapInvertedRoaringSetRange_Reader_BitSet(ctx, b, pv)
		// if err_reader_bs == nil {
		// 	fmt.Printf("  ==> search reader bitset took [%s] op [%s] card [%d] size [%d]\n",
		// 		time.Since(t_reader_bs).String(), pv.operator.Name(),
		// 		dbm_reader_bs.docIDs.GetCardinality(), len(dbm_reader_bs.docIDs.ToBuffer()))
		// }
		// return dbm_reader_bs, err_reader_bs

	case lsmkv.StrategyMapCollection:
		return s.docBitmapInvertedMap(ctx, b, limit, pv)
	default:
		return docBitmap{}, fmt.Errorf("property '%s' is neither filterable nor searchable nor rangeable", pv.prop)
	}
}

func (s *Searcher) docBitmapInvertedRoaringSet(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newUninitializedDocBitmap()
	isEmpty := true
	var readFn ReadFn = func(k []byte, docIDs *sroar.Bitmap) (bool, error) {
		if isEmpty {
			out.docIDs = docIDs
			isEmpty = false
		} else {
			out.docIDs.Or(docIDs)
		}

		// NotEqual requires the full set of potentially existing doc ids
		if pv.operator == filters.OperatorNotEqual {
			return true, nil
		}

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	rr := NewRowReaderRoaringSet(b, pv.value, pv.operator, false, s.bitmapFactory)
	if err := rr.Read(ctx, readFn); err != nil {
		return out, fmt.Errorf("read row: %w", err)
	}

	if isEmpty {
		return newDocBitmap(), nil
	}
	return out, nil
}

func (s *Searcher) docBitmapInvertedRoaringSetRange_Cursor(ctx context.Context, b *lsmkv.Bucket,
	pv *propValuePair,
) (docBitmap, error) {
	if len(pv.value) != 8 {
		return newDocBitmap(), fmt.Errorf("readerRoaringSetRange: invalid value length %d, should be 8 bytes", len(pv.value))
	}

	reader := lsmkv.NewBucketReaderRoaringSetRange(b.CursorRoaringSetRange, s.logger)

	docIds, err := reader.Read(ctx, binary.BigEndian.Uint64(pv.value), pv.operator)
	if err != nil {
		return newDocBitmap(), fmt.Errorf("readerRoaringSetRange: %w", err)
	}

	out := newUninitializedDocBitmap()
	out.docIDs = docIds
	return out, nil
}

func (s *Searcher) docBitmapInvertedRoaringSetRange3(ctx context.Context, b *lsmkv.Bucket,
	pv *propValuePair,
) (docBitmap, error) {
	if len(pv.value) != 8 {
		return newDocBitmap(), fmt.Errorf("readerRoaringSetRange: invalid value length %d, should be 8 bytes", len(pv.value))
	}

	reader := lsmkv.NewBucketReaderRoaringSetRange2(b.CursorRoaringSetRange2, s.logger)

	docIds, err := reader.Read(ctx, binary.BigEndian.Uint64(pv.value), pv.operator)
	if err != nil {
		return newDocBitmap(), fmt.Errorf("readerRoaringSetRange: %w", err)
	}

	out := newUninitializedDocBitmap()
	out.docIDs = docIds
	return out, nil
}

func (s *Searcher) docBitmapInvertedRoaringSetRange_Reader(ctx context.Context, b *lsmkv.Bucket,
	pv *propValuePair,
) (docBitmap, error) {
	if len(pv.value) != 8 {
		return newDocBitmap(), fmt.Errorf("readerRoaringSetRange: invalid value length %d, should be 8 bytes", len(pv.value))
	}

	reader := b.ReaderRoaringSetRange()
	defer reader.Close()

	docIds, err := reader.Read(ctx, binary.BigEndian.Uint64(pv.value), pv.operator)
	if err != nil {
		return newDocBitmap(), fmt.Errorf("readerRoaringSetRange: %w", err)
	}

	out := newUninitializedDocBitmap()
	out.docIDs = docIds
	return out, nil
}

func (s *Searcher) docBitmapInvertedRoaringSetRange_Cursor_BitSet(ctx context.Context, b *lsmkv.Bucket,
	pv *propValuePair,
) (docBitmap, error) {
	if len(pv.value) != 8 {
		return newDocBitmap(), fmt.Errorf("readerRoaringSetRange: invalid value length %d, should be 8 bytes", len(pv.value))
	}

	reader := lsmkv.NewBucketReaderRoaringSetRangeBS(b.CursorRoaringSetRangeBS, s.logger)

	bitset, err := reader.Read(ctx, binary.BigEndian.Uint64(pv.value), pv.operator)
	if err != nil {
		return newDocBitmap(), fmt.Errorf("readerRoaringSetRange: %w", err)
	}

	out := newUninitializedDocBitmap()
	t := time.Now()
	out.docIDs = bitset.ToSroar()
	fmt.Printf("  ==> conversion from bitset to sroar took [%s]\n", time.Since(t))
	return out, nil
}

func (s *Searcher) docBitmapInvertedRoaringSetRange_Reader_BitSet(ctx context.Context, b *lsmkv.Bucket,
	pv *propValuePair,
) (docBitmap, error) {
	if len(pv.value) != 8 {
		return newDocBitmap(), fmt.Errorf("readerRoaringSetRange: invalid value length %d, should be 8 bytes", len(pv.value))
	}

	reader := b.ReaderRoaringSetRangeBS()
	defer reader.Close()

	bitset, err := reader.Read(ctx, binary.BigEndian.Uint64(pv.value), pv.operator)
	if err != nil {
		return newDocBitmap(), fmt.Errorf("readerRoaringSetRange: %w", err)
	}

	out := newUninitializedDocBitmap()
	t := time.Now()
	out.docIDs = bitset.ToSroar()
	fmt.Printf("  ==> conversion from bitset to sroar took [%s]\n", time.Since(t))
	return out, nil
}

func (s *Searcher) docBitmapInvertedSet(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newUninitializedDocBitmap()
	isEmpty := true
	var readFn ReadFn = func(k []byte, ids *sroar.Bitmap) (bool, error) {
		if isEmpty {
			out.docIDs = ids
			isEmpty = false
		} else {
			out.docIDs.Or(ids)
		}

		// NotEqual requires the full set of potentially existing doc ids
		if pv.operator == filters.OperatorNotEqual {
			return true, nil
		}

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	rr := NewRowReader(b, pv.value, pv.operator, false, s.bitmapFactory)
	if err := rr.Read(ctx, readFn); err != nil {
		return out, fmt.Errorf("read row: %w", err)
	}

	if isEmpty {
		return newDocBitmap(), nil
	}
	return out, nil
}

func (s *Searcher) docBitmapInvertedMap(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newUninitializedDocBitmap()
	isEmpty := true
	var readFn ReadFn = func(k []byte, ids *sroar.Bitmap) (bool, error) {
		if isEmpty {
			out.docIDs = ids
			isEmpty = false
		} else {
			out.docIDs.Or(ids)
		}

		// NotEqual requires the full set of potentially existing doc ids
		if pv.operator == filters.OperatorNotEqual {
			return true, nil
		}

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	rr := NewRowReaderFrequency(b, pv.value, pv.operator, false, s.shardVersion, s.bitmapFactory)
	if err := rr.Read(ctx, readFn); err != nil {
		return out, fmt.Errorf("read row: %w", err)
	}

	if isEmpty {
		return newDocBitmap(), nil
	}
	return out, nil
}

func (s *Searcher) docBitmapGeo(ctx context.Context, pv *propValuePair) (docBitmap, error) {
	out := newDocBitmap()
	propIndex, ok := s.propIndices.ByProp(pv.prop)

	if !ok {
		return out, nil
	}

	res, err := propIndex.GeoIndex.WithinRange(ctx, *pv.valueGeoRange)
	if err != nil {
		return out, fmt.Errorf("geo index range search on prop %q: %w", pv.prop, err)
	}

	out.docIDs.SetMany(res)
	return out, nil
}
