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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

type binPersister[T float32 | byte | uint64] struct {
}

func newFloatBinPersister() *binPersister[float32] {
	return &binPersister[float32]{}
}

func newByteBinPersister() *binPersister[byte] {
	return &binPersister[byte]{}
}

func newUintBinPersister() *binPersister[uint64] {
	return &binPersister[uint64]{}
}

func (bp *binPersister[T]) addVectorToBin(binId int, id uint64, vector []T) {
	name := bp.nameForBin(binId)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Couldn't open file: ", name, " with error: ", err)
	}
	defer f.Close()
	err = binary.Write(f, binary.LittleEndian, id)
	if err != nil {
		log.Fatal("Write failed")
	}
	err = binary.Write(f, binary.LittleEndian, vector)
	if err != nil {
		log.Fatal("Write failed")
	}
}

func (bp *binPersister[T]) getBin(binId int) map[uint64][]T {
	f, err := os.OpenFile(bp.nameForBin(binId), os.O_RDONLY, 0644)
	if err != nil {
		log.Fatal("Couldn't open file")
	}
	defer f.Close()
	buf := make([]byte, 100*4*1536)
	result := make(map[uint64][]T)
	reader := bytes.NewReader(buf)
	var n int = 1
	for err == nil && n > 0 {
		n, err = f.Read(buf)
		var keyBuf uint64
		valBuf := make([]T, 1536)
		for i := 0; i < n/100/4/1536; i++ {
			binary.Read(reader, binary.LittleEndian, &keyBuf)
			binary.Read(reader, binary.LittleEndian, &valBuf)
			result[keyBuf] = valBuf
		}
		buf = buf[n:]
	}
	if err != io.EOF && err != nil {
		log.Fatal(err)
		return nil
	}
	return result
}

func (bp *binPersister[T]) updateBin(binId int, vectors map[uint64][]T) {
	bp.addBin(binId, vectors)
}

func (bp *binPersister[T]) addBin(binId int, vectors map[uint64][]T) {
	name := bp.nameForBin(binId)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Couldn't open file: ", name, " with error: ", err)
	}
	defer f.Close()
	for key, val := range vectors {
		err = binary.Write(f, binary.LittleEndian, key)
		if err != nil {
			log.Fatal("Write failed")
		}
		err = binary.Write(f, binary.LittleEndian, val)
		if err != nil {
			log.Fatal("Write failed")
		}
	}
}

func (bp *binPersister[T]) nameForBin(binId int) string {
	return fmt.Sprint(binId, ".bin")
}
