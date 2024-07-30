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

	// fmt.Printf("analysis started [%s]\n", s.path)

	// arTotalAddCount := 0
	// arTotalAddCard := 0
	// bmTotalAddCount := 0
	// bmTotalAddCard := 0

	// arTotalDelCount := 0
	// arTotalDelCard := 0
	// bmTotalDelCount := 0
	// bmTotalDelCard := 0

	// for key, layer, ok := segmentCursor.First(); ok; key, layer, ok = segmentCursor.Next() {
	// 	if layer.Additions != nil {
	// 		arCount, arCard, bmCount, bmCard := layer.Additions.Stats()
	// 		fmt.Printf("[%d] key additions\n"+
	// 			"    array count [%d] card [%d] avg.card [%.3f]\n"+
	// 			"    bitmap count [%d] card [%d] avg.card [%.3f]\n"+
	// 			"    bm:ar ratio count [%.3f] card [%.3f]\n",
	// 			key, arCount, arCard, float32(arCard)/float32(arCount),
	// 			bmCount, bmCard, float32(bmCard)/float32(bmCount),
	// 			float32(bmCount)/float32(bmCount+arCount), float32(bmCard)/float32(bmCard+arCard))

	// 		arTotalAddCount += arCount
	// 		arTotalAddCard += arCard
	// 		bmTotalAddCount += bmCount
	// 		bmTotalAddCard += bmCard
	// 	}
	// 	if layer.Deletions != nil {
	// 		arCount, arCard, bmCount, bmCard := layer.Deletions.Stats()
	// 		fmt.Printf("[%d] key deletions\n"+
	// 			"    array count [%d] card [%d] avg.card [%.3f]\n"+
	// 			"    bitmap count [%d] card [%d] avg.card [%.3f]\n"+
	// 			"    bm:ar ratio count [%.3f] card [%.3f]\n",
	// 			key, arCount, arCard, float32(arCard)/float32(arCount),
	// 			bmCount, bmCard, float32(bmCard)/float32(bmCount),
	// 			float32(bmCount)/float32(bmCount+arCount), float32(bmCard)/float32(bmCard+arCard))

	// 		arTotalDelCount += arCount
	// 		arTotalDelCard += arCard
	// 		bmTotalDelCount += bmCount
	// 		bmTotalDelCard += bmCard
	// 	}
	// }
	// fmt.Printf("analysis finished [%s]\n"+
	// 	"    add array count [%d] card [%d] avg.card [%.3f]\n"+
	// 	"    add bitmap count [%d] card [%d] avg.card [%.3f]\n"+
	// 	"    add bm:ar ratio count [%.3f] card [%.3f]\n"+
	// 	"    del array count [%d] card [%d] avg.card [%.3f]\n"+
	// 	"    del bitmap count [%d] card [%d] avg.card [%.3f]\n"+
	// 	"    add bm:ar ratio count [%.3f] card [%.3f]\n\n",
	// 	s.path, arTotalAddCount, arTotalAddCard, float32(arTotalAddCard)/float32(arTotalAddCount),
	// 	bmTotalAddCount, bmTotalAddCard, float32(bmTotalAddCard)/float32(bmTotalAddCount),
	// 	float32(bmTotalAddCount)/float32(bmTotalAddCount+arTotalAddCount),
	// 	float32(bmTotalAddCard)/float32(bmTotalAddCard+arTotalAddCard),
	// 	arTotalDelCount, arTotalDelCard, float32(arTotalDelCard)/float32(arTotalDelCount),
	// 	bmTotalDelCount, bmTotalDelCard, float32(bmTotalDelCard)/float32(bmTotalDelCount),
	// 	float32(bmTotalDelCount)/float32(bmTotalDelCount+arTotalDelCount),
	// 	float32(bmTotalDelCard)/float32(bmTotalDelCard+arTotalDelCard))

	gaplessSegmentCursor := roaringsetrange.NewGaplessSegmentCursor(segmentCursor)
	return roaringsetrange.NewSegmentReader(gaplessSegmentCursor)
}

// func (ra *Bitmap) Stats() (int, int, int, int) {
// 	bmCount := 0
// 	arCount := 0

// 	bmCard := 0
// 	arCard := 0

// 	l := ra.keys.numKeys()
// 	for i := 0; i < l; i++ {
// 		off := ra.keys.val(i)
// 		c := ra.getContainer(off)
// 		card := getCardinality(c)

// 		if c[indexType] == typeBitmap {
// 			bmCount++
// 			bmCard += card
// 		}
// 		if c[indexType] == typeArray {
// 			arCount++
// 			arCard += card
// 		}
// 	}

// 	return arCount, arCard, bmCount, bmCard
// }

func (sg *SegmentGroup) newRoaringSetRangeReaders() ([]roaringsetrange.InnerReader, func()) {
	sg.maintenanceLock.RLock()

	readers := make([]roaringsetrange.InnerReader, len(sg.segments))
	for i, segment := range sg.segments {
		readers[i] = segment.newRoaringSetRangeReader()
	}

	return readers, sg.maintenanceLock.RUnlock
}

func (s *segment) newRoaringSetRangeCursor2() roaringsetrange.InnerCursor {
	return roaringsetrange.NewGaplessSegmentCursor(
		roaringsetrange.NewSegmentCursor(s.contents[s.dataStartPos:s.dataEndPos]),
	)
}

func (sg *SegmentGroup) newRoaringSetRangeCursors2() ([]roaringsetrange.InnerCursor, func()) {
	sg.maintenanceLock.RLock()

	cursors := make([]roaringsetrange.InnerCursor, len(sg.segments))
	for i, segment := range sg.segments {
		cursors[i] = segment.newRoaringSetRangeCursor2()
	}

	return cursors, sg.maintenanceLock.RUnlock
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
