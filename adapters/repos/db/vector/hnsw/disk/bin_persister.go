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

package disk

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
)

type binPersister[T float32 | byte | uint64] struct {
	dims int
	path string
}

func newFloatBinPersister(path string, dims int) *binPersister[float32] {
	return &binPersister[float32]{
		dims: dims,
		path: path,
	}
}

func newByteBinPersister(path string, dims int) *binPersister[byte] {
	return &binPersister[byte]{
		dims: dims,
		path: path,
	}
}

func newUintBinPersister(path string, dims int) *binPersister[uint64] {
	return &binPersister[uint64]{
		dims: dims,
		path: path,
	}
}

func (bp *binPersister[T]) addVectorToBin(binId int, id uint64, vector []T) {
	name := bp.nameForBin(binId)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Couldn't open file: ", name, " with error: ", err)
	}
	defer f.Close()
	buff := make([]byte, bp.dims*4+8)
	binary.LittleEndian.PutUint64(buff, id)
	for i := 0; i < len(vector); i++ {
		binary.LittleEndian.PutUint32(buff[8+i*4:], math.Float32bits(float32(vector[i])))
	}
	f.Write(buff)
}

func (bp *binPersister[T]) getRawBin(binId int) []byte {
	f, err := os.OpenFile(bp.nameForBin(binId), os.O_RDONLY, 0644)
	if err != nil {
		log.Fatal("Couldn't open file")
	}
	defer f.Close()
	buff := make([]byte, 100*(4*bp.dims+8))
	n, err := f.Read(buff)
	if err != io.EOF && err != nil {
		log.Fatal(err)
		return nil
	}
	return buff[:n]
}

func (bp *binPersister[T]) getBin(binId int) map[uint64][]T {
	f, err := os.OpenFile(bp.nameForBin(binId), os.O_RDONLY, 0644)
	if err != nil {
		log.Fatal("Couldn't open file")
	}
	defer f.Close()
	buff := make([]byte, 100*(4*bp.dims+8))
	result := make(map[uint64][]T)
	var n int = 1
	for err == nil && n > 0 {
		n, err = f.Read(buff)
		for i := 0; i < n/(4*bp.dims+8); i++ {
			valBuf := make([]T, bp.dims)
			key := binary.LittleEndian.Uint64(buff[i*(bp.dims*4+8):])
			for j := 0; j < bp.dims; j++ {
				valBuf[j] = T(math.Float32frombits(binary.LittleEndian.Uint32(buff[i*(bp.dims*4+8)+8+j*4:])))
			}
			result[key] = valBuf
		}
	}
	if err != io.EOF && err != nil {
		log.Fatal(err)
		return nil
	}
	return result
}

func (bp *binPersister[T]) addBin(binId int, vectors map[uint64][]T) {
	name := bp.nameForBin(binId)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Couldn't open file: ", name, " with error: ", err)
	}
	defer f.Close()
	buff := make([]byte, len(vectors)*(bp.dims*4+8))
	for key, val := range vectors {
		binary.LittleEndian.PutUint64(buff, key)
		for i := 0; i < len(val); i++ {
			binary.LittleEndian.PutUint32(buff[8+i*4:], math.Float32bits(float32(val[i])))
		}
	}
	n, err := f.Write(buff)
	for err != nil && n > 0 {
		buff = buff[n:]
		n, err = f.Write(buff)
	}
}

func (bp *binPersister[T]) nameForBin(binId int) string {
	return filepath.Join(bp.path, fmt.Sprint(binId, ".bin"))
}
