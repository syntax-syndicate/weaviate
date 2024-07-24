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

func (s *segment) newRoaringSetRangeReader() *roaringsetrange.SegmentReader {
	segmentCursor := roaringsetrange.NewSegmentCursor(s.contents[s.dataStartPos:s.dataEndPos])
	gaplessSegmentCursor := roaringsetrange.NewGaplessSegmentCursor(segmentCursor)
	return roaringsetrange.NewSegmentReader(gaplessSegmentCursor)
}

func (sg *SegmentGroup) newRoaringSetRangeReaders() ([]roaringsetrange.InnerReader, func()) {
	sg.maintenanceLock.RLock()

	readers := make([]roaringsetrange.InnerReader, len(sg.segments))
	for i, segment := range sg.segments {
		readers[i] = segment.newRoaringSetRangeReader()
	}

	return readers, sg.maintenanceLock.RUnlock
}

func (s *segment) newRoaringSetRangeCursor() *roaringsetrange.SegmentCursor {
	return roaringsetrange.NewSegmentCursor(s.contents[s.dataStartPos:s.dataEndPos])
}

func (sg *SegmentGroup) newRoaringSetRangeCursors() ([]roaringsetrange.InnerCursor, func()) {
	sg.maintenanceLock.RLock()

	cursors := make([]roaringsetrange.InnerCursor, len(sg.segments))
	// readers := make([]*roaringsetrange.SegmentReader, len(sg.segments))
	for i, segment := range sg.segments {
		cursors[i] = segment.newRoaringSetRangeCursor()
		// readers[i] = segment.newRoaringSetRangeReader()
	}

	// cursor := cursors[0]
	// reader := readers[0]
	// value := uint64(13891384759934908578)

	// start := time.Now()

	// // bit, layer, ok := cursor.First()
	// // resBM := layer.Additions.Clone()

	// // var d_or, d_and, d_cond time.Duration
	// // for bit, layer, ok = cursor.Next(); ok; bit, layer, ok = cursor.Next() {
	// // 	// add := sroar.NewBitmap()
	// // 	// add.Or(layer.Additions)
	// // 	add := layer.Additions

	// // 	fmt.Printf("  ==> bit [%d] res card [%d] min [%d] max [%d] size [%d]\n",
	// // 		bit, resBM.GetCardinality(), resBM.Minimum(), resBM.Maximum(), len(resBM.ToBuffer()))
	// // 	fmt.Printf("  ==> bit [%d] add card [%d] min [%d] max [%d] size [%d]\n",
	// // 		bit, add.GetCardinality(), add.Minimum(), add.Maximum(), len(add.ToBuffer()))

	// // 	// fmt.Printf("  ==> Add bit [%d] card [%d]\n", bit, layer.Additions.GetCardinality())
	// // 	// fmt.Printf("  ==> Add bit [%d] contains A res [%v] contains B res [%v]\n",
	// // 	// 	bit, layer.Additions.Contains(331965), layer.Additions.Contains(291321649))
	// // 	s := time.Now()
	// // 	var d1, d2 time.Duration
	// // 	if value&(1<<(bit-1)) != 0 {
	// // 		// cr := resBM.GetCardinality()
	// // 		// cc := ctrBM.GetCardinality()
	// // 		// fmt.Printf("  ==> AND bit [%d] card res [%d] card ctr [%d]\n", bit, cr, cc)
	// // 		// fmt.Printf("  ==> AND bit [%d] contains A res [%v] contains B res [%v] contains A ctr [%v] contains B ctr [%v] \n\n",
	// // 		// 	bit, resBM.Contains(331965), resBM.Contains(291321649), ctrBM.Contains(331965), ctrBM.Contains(291321649))

	// // 		// resBM = sroar.And(resBM, layer.Additions)
	// // 		// resBM = sroar.And(layer.Additions, resBM)
	// // 		resBM.And(add)
	// // 		d1 = time.Since(s)
	// // 		d_and += d1

	// // 		s = time.Now()
	// // 		resBM = roaringset.Condense(resBM)
	// // 		d2 = time.Since(s)
	// // 		d_cond += d2
	// // 		// resBM.And(layer.Additions)

	// // 		fmt.Printf("  ==> bit [%d] AND took [%s]\n\n", bit, (d1 + d2).String())

	// // 	} else {
	// // 		// cr := resBM.GetCardinality()
	// // 		// cc := ctrBM.GetCardinality()
	// // 		// fmt.Printf("  ==> OR bit [%d] card res [%d] card ctr [%d]\n", bit, cr, cc)
	// // 		// fmt.Printf("  ==> OR bit [%d] contains A res [%v] contains B res [%v] contains A ctr [%v] contains B ctr [%v] \n\n",
	// // 		// 	bit, resBM.Contains(331965), resBM.Contains(291321649), ctrBM.Contains(331965), ctrBM.Contains(291321649))

	// // 		// resBM = sroar.Or(resBM, layer.Additions)
	// // 		// resBM = sroar.Or(layer.Additions, resBM)
	// // 		resBM.Or(add)
	// // 		d1 = time.Since(s)
	// // 		d_or += d1

	// // 		fmt.Printf("  ==> bit [%d] OR took [%s]\n\n", bit, d1.String())

	// // 		// resBM.Or(layer.Additions)
	// // 	}

	// // 	// cr := resBM.GetCardinality()
	// // 	// cc := ctrBM.GetCardinality()
	// // 	// if cr != cc {
	// // 	// 	fmt.Printf("  ==> diff bit [%d] card res [%d] card ctr [%d]\n", bit, cr, cc)
	// // 	// 	fmt.Printf("  ==> diff bit [%d] contains A res [%v] contains B res [%v] contains A ctr [%v] contains B ctr [%v] \n",
	// // 	// 		bit, resBM.Contains(331965), resBM.Contains(291321649), ctrBM.Contains(331965), ctrBM.Contains(291321649))
	// // 	// 	fmt.Printf("  ==> diff bit [%d] min res [%d] max res [%d] min ctr [%d] max ctr [%d]\n\n",
	// // 	// 		bit, resBM.Minimum(), resBM.Maximum(), ctrBM.Minimum(), ctrBM.Maximum())
	// // 	// 	break

	// // 	// }

	// // 	// s = time.Now()
	// // 	// resBM = roaringset.Condense(resBM)
	// // 	// d2 += time.Since(s)

	// // }
	// // d_total := time.Since(start)

	// // fmt.Printf("  ==> cursor gte took total [%s] or [%s] and [%s] condensing [%s]\n      card [%d] size [%d]\n\n",
	// // 	d_total.String(), d_or.String(), d_and.String(), d_cond.String(),
	// // 	resBM.GetCardinality(), len(resBM.ToBuffer()))

	// start = time.Now()
	// res, _ := reader.Read(context.Background(), value, filters.OperatorGreaterThanEqual)
	// fmt.Printf("  ==> reader gte took total [%s]\n      card [%d] size [%d]\n\n",
	// 	time.Since(start).String(),
	// 	res.Additions.GetCardinality(), len(res.Additions.ToBuffer()))

	// start = time.Now()
	// res, _ = reader.Read(context.Background(), value, filters.OperatorGreaterThan)
	// fmt.Printf("  ==> reader gt took total [%s]\n      card [%d] size [%d]\n\n",
	// 	time.Since(start).String(),
	// 	res.Additions.GetCardinality(), len(res.Additions.ToBuffer()))

	// start = time.Now()
	// res, _ = reader.Read(context.Background(), value, filters.OperatorLessThanEqual)
	// fmt.Printf("  ==> reader lte took total [%s]\n      card [%d] size [%d]\n\n",
	// 	time.Since(start).String(),
	// 	res.Additions.GetCardinality(), len(res.Additions.ToBuffer()))

	// start = time.Now()
	// res, _ = reader.Read(context.Background(), value, filters.OperatorLessThan)
	// fmt.Printf("  ==> reader lt took total [%s]\n      card [%d] size [%d]\n\n",
	// 	time.Since(start).String(),
	// 	res.Additions.GetCardinality(), len(res.Additions.ToBuffer()))

	// start = time.Now()
	// res, _ = reader.Read(context.Background(), value, filters.OperatorEqual)
	// fmt.Printf("  ==> reader eq took total [%s]\n      card [%d] size [%d]\n\n",
	// 	time.Since(start).String(),
	// 	res.Additions.GetCardinality(), len(res.Additions.ToBuffer()))

	// start = time.Now()
	// res, _ = reader.Read(context.Background(), value, filters.OperatorNotEqual)
	// fmt.Printf("  ==> reader neq took total [%s]\n      card [%d] size [%d]\n\n",
	// 	time.Since(start).String(),
	// 	res.Additions.GetCardinality(), len(res.Additions.ToBuffer()))

	return cursors, sg.maintenanceLock.RUnlock
}
