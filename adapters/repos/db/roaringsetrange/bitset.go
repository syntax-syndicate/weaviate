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
	"fmt"
	"log"
	"math/bits"
	"reflect"
	"strings"
	"unsafe"

	"github.com/weaviate/sroar"
)

const (
	nBytes    = 16
	bitSetMax = 500_000_000
)

var masks [nBytes]uint16

func init() {
	for i := 0; i < nBytes; i++ {
		masks[i] = 1 << i
	}
}

type BitSet []uint16

func NewBitSet(max int) BitSet {
	size := max/nBytes + 1
	return make([]uint16, size)
}

func NewDefaultBitSet() BitSet {
	return NewBitSet(bitSetMax)
}

func (bs BitSet) Set(x ...uint64) BitSet {
	for i := range x {
		k := x[i] / nBytes
		if k >= uint64(len(bs)) {
			log.Fatalf("element %d out of bitset range", x[i])
		}
		bs[k] |= masks[x[i]%nBytes]
	}
	return bs
}

func (bs BitSet) Remove(x ...uint64) BitSet {
	for i := range x {
		k := x[i] / nBytes
		if k >= uint64(len(bs)) {
			log.Fatalf("element %d out of bitset range", x[i])
		}
		bs[k] &^= masks[x[i]%nBytes]
	}
	return bs
}

func (bs BitSet) Clone() BitSet {
	c := make([]uint16, len(bs))
	copy(c, bs)
	return c
}

func (bs BitSet) And(a BitSet) BitSet {
	if len(a) > len(bs) {
		log.Fatalf("can not merge bigger set of size %d into smaller set of size %d", len(a), len(bs))
	}
	for i := range a {
		bs[i] &= a[i]
	}
	for i := len(a); i < len(bs); i++ {
		bs[i] &= 0
	}
	return bs
}

func (bs BitSet) Or(a BitSet) BitSet {
	if len(a) > len(bs) {
		log.Fatalf("can not merge bigger set of size %d into smaller set of size %d", len(a), len(bs))
	}
	for i := range a {
		bs[i] |= a[i]
	}
	return bs
}

func (bs BitSet) AndNot(a BitSet) BitSet {
	if len(a) > len(bs) {
		log.Fatalf("can not merge bigger set of size %d into smaller set of size %d", len(a), len(bs))
	}
	for i := range a {
		bs[i] &^= a[i]
	}
	return bs
}

func (bs BitSet) Cardinality() int {
	c := 0
	for i := range bs {
		c += bits.OnesCount16(bs[i])
	}
	return c
}

func (bs BitSet) IsEmpty() bool {
	for i := range bs {
		if bits.OnesCount16(bs[i]) > 0 {
			return false
		}
	}
	return true
}

func (bs BitSet) String() string {
	sb := &strings.Builder{}
	for i := len(bs) - 1; i >= 0; i-- {
		sb.WriteString(fmt.Sprintf("%016b", bs[i]))
		if i > 0 {
			sb.WriteString(" ")
		}
	}
	return sb.String()
}

func (bs BitSet) ToArray() []uint64 {
	a := make([]uint64, bs.Cardinality())
	k := 0
	bs.iterate(func(x uint64) {
		a[k] = x
		k++
	})
	return a
}

func (bs BitSet) ToBuffer() []byte {
	return toByteSlice(bs)
}

func BitSetFromBuffer(data []byte) BitSet {
	if len(data)%2 != 0 || len(data) < 8 {
		log.Fatalf("invalid data length")
	}
	return toUint16Slice(data)
}

func (bs BitSet) ToSroar() *sroar.Bitmap {
	bm := sroar.NewBitmap()
	bs.iterate(func(x uint64) {
		bm.Set(x)
	})
	return bm
}

func BitSetFromSroar(max int, bm *sroar.Bitmap) BitSet {
	if bm.Maximum() > uint64(max) {
		log.Fatalf("max element of bitmap exceed max bitset size")
	}
	bs := NewBitSet(max)
	if bm.IsEmpty() {
		return bs
	}
	it := bm.NewIterator()
	bs.Set(it.Next())

	for x := it.Next(); x != 0; x = it.Next() {
		bs.Set(x)
	}

	return bs
}

func BitSetDefaultFromSroar(bm *sroar.Bitmap) BitSet {
	return BitSetFromSroar(bitSetMax, bm)
}

func (bs BitSet) iterate(callback func(x uint64)) {
	for i := 0; i < len(bs); i++ {
		for j := 0; j < nBytes; j++ {
			if bs[i]&masks[j] > 0 {
				callback(uint64(i)*16 + uint64(j))
			}
		}
	}
}

func toByteSlice(b []uint16) []byte {
	// reference: https://go101.org/article/unsafe.html
	var bs []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&bs))
	hdr.Len = len(b) * 2
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return bs
}

func toUint16Slice(b []byte) []uint16 {
	var u16s []uint16
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u16s))
	hdr.Len = len(b) / 2
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u16s
}

type BitSetLayer struct {
	Additions BitSet
	Deletions BitSet
}

type BitSetLayers []BitSetLayer

func (bsl BitSetLayers) FlattenMutate() BitSet {
	if len(bsl) == 0 {
		return NewDefaultBitSet()
	}

	merged := bsl[0].Additions
	for i := 1; i < len(bsl); i++ {
		merged.AndNot(bsl[i].Deletions)
		merged.Or(bsl[i].Additions)
	}

	return merged
}
