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
	"sync"
	"unsafe"

	"github.com/weaviate/sroar"
)

var masks64 [64]uint64

const conc = 4

func init() {
	for i := 0; i < 64; i++ {
		masks64[i] = 1 << i
	}
}

type BitSet64 []uint64

func NewBitSet64(max int) BitSet64 {
	size := max/64 + 1
	return make([]uint64, size)
}

func NewDefaultBitSet64() BitSet64 {
	return NewBitSet64(bitSetMax)
}

func (bs BitSet64) Set(x ...uint64) BitSet64 {
	for i := range x {
		k := x[i] / 64
		if k >= uint64(len(bs)) {
			log.Fatalf("element %d out of bitset range", x[i])
		}
		bs[k] |= masks64[x[i]%64]
	}
	return bs
}

func (bs BitSet64) Remove(x ...uint64) BitSet64 {
	for i := range x {
		k := x[i] / 64
		if k >= uint64(len(bs)) {
			log.Fatalf("element %d out of bitset range", x[i])
		}
		bs[k] &^= masks64[x[i]%64]
	}
	return bs
}

func (bs BitSet64) Clone() BitSet64 {
	c := make([]uint64, len(bs))
	copy(c, bs)
	return c
}

func (bs BitSet64) And(a BitSet64) BitSet64 {
	// TODO
	if len(a) > len(bs) {
		log.Fatalf("can not merge bigger set of size %d into smaller set of size %d", len(a), len(bs))
	}

	delta := len(bs)/conc + 1

	wg := new(sync.WaitGroup)
	wg.Add(conc - 1)

	for c := 0; c < conc-1; c++ {
		j := c * delta
		l := (c + 1) * delta
		go func() {
			defer wg.Done()

			for i := j; i < l; i++ {
				bs[i] &= a[i]
			}
		}()
	}

	for i := (conc - 1) * delta; i < len(bs); i++ {
		bs[i] &= a[i]
	}

	wg.Wait()

	// ====

	// delta := len(bs)/conc + 1

	// wg := new(sync.WaitGroup)
	// wg.Add(conc)

	// for c := 0; c < conc; c++ {
	// 	j := c * delta
	// 	l := (c + 1) * delta
	// 	if l > len(bs) {
	// 		l = len(bs)
	// 	}
	// 	go func() {
	// 		defer wg.Done()

	// 		for i := j; i < l; i++ {
	// 			bs[i] &= a[i]
	// 		}
	// 	}()
	// }

	// wg.Wait()

	// ====

	// for i := range a {
	// 	bs[i] &= a[i]
	// }
	// for i := len(a); i < len(bs); i++ {
	// 	bs[i] = 0
	// }
	return bs
}

func (bs BitSet64) Or(a BitSet64) BitSet64 {
	// TODO
	if len(a) > len(bs) {
		log.Fatalf("can not merge bigger set of size %d into smaller set of size %d", len(a), len(bs))
	}

	delta := len(bs)/conc + 1

	wg := new(sync.WaitGroup)
	wg.Add(conc - 1)

	for c := 0; c < conc-1; c++ {
		j := c * delta
		l := (c + 1) * delta
		go func() {
			defer wg.Done()

			for i := j; i < l; i++ {
				bs[i] |= a[i]
			}
		}()
	}

	for i := (conc - 1) * delta; i < len(bs); i++ {
		bs[i] |= a[i]
	}

	wg.Wait()

	// ====

	// delta := len(bs)/conc + 1

	// wg := new(sync.WaitGroup)
	// wg.Add(conc)

	// for c := 0; c < conc; c++ {
	// 	j := c * delta
	// 	l := (c + 1) * delta
	// 	if l > len(bs) {
	// 		l = len(bs)
	// 	}
	// 	go func() {
	// 		defer wg.Done()

	// 		for i := j; i < l; i++ {
	// 			bs[i] |= a[i]
	// 		}
	// 	}()
	// }

	// wg.Wait()

	// ====

	// for i := range a {
	// 	bs[i] |= a[i]
	// }
	return bs
}

func (bs BitSet64) AndNot(a BitSet64) BitSet64 {
	// TODO
	if len(a) > len(bs) {
		log.Fatalf("can not merge bigger set of size %d into smaller set of size %d", len(a), len(bs))
	}

	delta := len(bs)/conc + 1

	wg := new(sync.WaitGroup)
	wg.Add(conc - 1)

	for c := 0; c < conc-1; c++ {
		j := c * delta
		l := (c + 1) * delta
		go func() {
			defer wg.Done()

			for i := j; i < l; i++ {
				bs[i] &^= a[i]
			}
		}()
	}

	for i := (conc - 1) * delta; i < len(bs); i++ {
		bs[i] &^= a[i]
	}

	wg.Wait()

	// ====

	// delta := len(bs)/conc + 1

	// wg := new(sync.WaitGroup)
	// wg.Add(conc)

	// for c := 0; c < conc; c++ {
	// 	j := c * delta
	// 	l := (c + 1) * delta
	// 	if l > len(bs) {
	// 		l = len(bs)
	// 	}
	// 	go func() {
	// 		defer wg.Done()

	// 		for i := j; i < l; i++ {
	// 			bs[i] &^= a[i]
	// 		}
	// 	}()
	// }

	// wg.Wait()

	// ====

	// for i := range a {
	// 	bs[i] &^= a[i]
	// }
	return bs
}

func (bs BitSet64) ReverseAndNot(a BitSet64) BitSet64 {
	// TODO
	if len(a) > len(bs) {
		log.Fatalf("can not merge bigger set of size %d into smaller set of size %d", len(a), len(bs))
	}

	delta := len(bs)/conc + 1

	wg := new(sync.WaitGroup)
	wg.Add(conc - 1)

	for c := 0; c < conc-1; c++ {
		j := c * delta
		l := (c + 1) * delta
		go func() {
			defer wg.Done()

			for i := j; i < l; i++ {
				bs[i] = a[i] &^ bs[i]
			}
		}()
	}

	for i := (conc - 1) * delta; i < len(bs); i++ {
		bs[i] = a[i] &^ bs[i]
	}

	wg.Wait()

	// delta := len(bs)/conc + 1

	// wg := new(sync.WaitGroup)
	// wg.Add(conc)

	// for c := 0; c < conc; c++ {
	// 	j := c * delta
	// 	l := (c + 1) * delta
	// 	if l > len(bs) {
	// 		l = len(bs)
	// 	}
	// 	go func() {
	// 		defer wg.Done()

	// 		for i := j; i < l; i++ {
	// 			bs[i] = a[i] &^ bs[i]
	// 		}
	// 	}()
	// }

	// wg.Wait()

	// ====

	// for i := range a {
	// 	bs[i] = a[i] &^ bs[i]
	// }
	// for i := len(a); i < len(bs); i++ {
	// 	bs[i] = 0
	// }
	return bs
}

func (bs BitSet64) Cardinality() int {
	c := 0
	for i := range bs {
		c += bits.OnesCount64(bs[i])
	}
	return c
}

func (bs BitSet64) IsEmpty() bool {
	for i := range bs {
		if bits.OnesCount64(bs[i]) > 0 {
			return false
		}
	}
	return true
}

func (bs BitSet64) String() string {
	sb := &strings.Builder{}
	for i := len(bs) - 1; i >= 0; i-- {
		sb.WriteString(fmt.Sprintf("%064b", bs[i]))
		if i > 0 {
			sb.WriteString(" ")
		}
	}
	return sb.String()
}

func (bs BitSet64) ToArray() []uint64 {
	a := make([]uint64, bs.Cardinality())
	k := 0
	bs.iterate(func(x uint64) {
		a[k] = x
		k++
	})
	return a
}

func (bs BitSet64) ToBuffer() []byte {
	return toByteSlice64(bs)
}

func BitSet64FromBuffer(data []byte) BitSet64 {
	if len(data)%8 != 0 || len(data) < 64 {
		log.Fatalf("invalid data length")
	}
	return toUint64Slice(data)
}

func (bs BitSet64) ToSroar() *sroar.Bitmap {
	bm := sroar.NewBitmap()
	bs.iterate(func(x uint64) {
		bm.Set(x)
	})
	return bm
}

func BitSet64FromSroar(max int, bm *sroar.Bitmap) BitSet {
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

func BitSet64DefaultFromSroar(bm *sroar.Bitmap) BitSet {
	return BitSetFromSroar(bitSetMax, bm)
}

func (bs BitSet64) iterate(callback func(x uint64)) {
	for i := 0; i < len(bs); i++ {
		for j := 0; j < 64; j++ {
			if bs[i]&masks64[j] > 0 {
				callback(uint64(i)*64 + uint64(j))
			}
		}
	}
}

func toByteSlice64(b []uint64) []byte {
	// reference: https://go101.org/article/unsafe.html
	var bs []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&bs))
	hdr.Len = len(b) * 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return bs
}

func toUint64Slice(b []byte) []uint64 {
	var u64s []uint64
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u64s))
	hdr.Len = len(b) / 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u64s
}
