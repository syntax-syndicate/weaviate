package flat

import (
	"encoding/binary"
	"path/filepath"
	"sync/atomic"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

const metadataFile = "flat.db"

var dimensionBucket = []byte("dimensions")

func (f *flat) initDimensions() {
	dims, err := f.fetchDimensions()
	if err != nil {
		f.logger.Warnf("could not fetch dimensions: %v", err)
	}
	if dims == 0 {
		dims = f.calculateDimensions()
	}
	if dims > 0 {
		f.trackDimensionsOnce.Do(func() {
			atomic.StoreInt32(&f.dims, dims)
		})
	}
}

func (f *flat) fetchDimensions() (int32, error) {
	path := filepath.Join(f.rootPath, metadataFile)

	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return 0, errors.Wrapf(err, "open %q", path)
	}
	defer db.Close()

	var dimensions int32 = 0
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dimensionBucket)
		if b == nil {
			return nil // Bucket doesn't exist, return default value (0)
		}
		v := b.Get([]byte("value"))
		if v == nil {
			return nil // Key doesn't exist, return default value (0)
		}
		dimensions = int32(binary.LittleEndian.Uint32(v))
		return nil
	})
	if err != nil {
		return 0, errors.Wrap(err, "fetch dimensions")
	}

	return dimensions, nil
}

func (index *flat) calculateDimensions() int32 {
	bucket := index.store.Bucket(index.getBucketName())
	if bucket == nil {
		return 0
	}
	cursor := bucket.Cursor()
	defer cursor.Close()

	var key []byte
	var v []byte
	const maxCursorSize = 100000
	i := 0
	for key, v = cursor.First(); key != nil; key, v = cursor.Next() {
		if len(v) > 0 {
			return int32(len(v) / 4)
		}
		if i > maxCursorSize {
			break
		}
		i++
	}
	return 0
}

func (f *flat) setDimensions(dimensions int32) error {
	path := filepath.Join(f.rootPath, metadataFile)

	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return errors.Wrapf(err, "open %q", path)
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(dimensionBucket)
		if err != nil {
			return errors.Wrap(err, "create bucket")
		}
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(dimensions))
		return b.Put([]byte("value"), buf)
	})
	if err != nil {
		return errors.Wrap(err, "set dimensions")
	}

	return nil
}
