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

package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/usecases/objects"
)

// return value map[int]error gives the error for the index as it received it
func (s *Shard) DeleteObjectBatch(ctx context.Context, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool) objects.BatchSimpleObjects {
	s.activityTracker.Add(1)
	if err := s.isReadOnly(); err != nil {
		return objects.BatchSimpleObjects{
			objects.BatchSimpleObject{Err: err},
		}
	}
	return newDeleteObjectsBatcher(s).Delete(ctx, uuids, deletionTime, dryRun)
}

type deleteObjectsBatcher struct {
	sync.Mutex
	shard   ShardLike
	objects objects.BatchSimpleObjects
}

func newDeleteObjectsBatcher(shard ShardLike) *deleteObjectsBatcher {
	return &deleteObjectsBatcher{shard: shard}
}

func (b *deleteObjectsBatcher) Delete(ctx context.Context, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool) objects.BatchSimpleObjects {
	b.delete(ctx, uuids, deletionTime, dryRun)
	b.flushWALs(ctx)
	return b.objects
}

func (b *deleteObjectsBatcher) delete(ctx context.Context, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool) {
	b.objects = b.deleteSingleBatchInLSM(ctx, uuids, deletionTime, dryRun)
}

func (b *deleteObjectsBatcher) deleteSingleBatchInLSM(ctx context.Context,
	batch []strfmt.UUID, deletionTime time.Time, dryRun bool,
) objects.BatchSimpleObjects {
	before := time.Now()
	defer b.shard.Metrics().BatchDelete(before, "shard_delete_all")

	result := make(objects.BatchSimpleObjects, len(batch))
	objLock := &sync.Mutex{}

	// if the context is expired fail all
	if err := ctx.Err(); err != nil {
		for i := range result {
			result[i] = objects.BatchSimpleObject{Err: errors.Wrap(err, "begin batch")}
		}
		return result
	}

	eg := enterrors.NewErrorGroupWrapper(b.shard.Index().logger)
	eg.SetLimit(_NUMCPU) // prevent unbounded concurrency

	for j, docID := range batch {
		index := j
		docID := docID
		f := func() error {
			// perform delete
			obj := b.deleteObjectOfBatchInLSM(ctx, docID, deletionTime, dryRun)
			objLock.Lock()
			result[index] = obj
			objLock.Unlock()
			return nil
		}
		eg.Go(f, index, docID)
	}
	// safe to ignore error, as the internal routines never return an error
	eg.Wait()

	return result
}

func (b *deleteObjectsBatcher) deleteObjectOfBatchInLSM(ctx context.Context,
	uuid strfmt.UUID, deletionTime time.Time, dryRun bool,
) objects.BatchSimpleObject {
	before := time.Now()
	defer b.shard.Metrics().BatchDelete(before, "shard_delete_individual_total")
	if !dryRun {
		err := b.shard.batchDeleteObject(ctx, uuid, deletionTime)
		return objects.BatchSimpleObject{UUID: uuid, Err: err}
	}

	return objects.BatchSimpleObject{UUID: uuid, Err: nil}
}

func (s *Shard) batchDeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error {
	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return err
	}

	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	existing, err := bucket.Get(idBytes)
	if err != nil {
		return errors.Wrap(err, "unexpected error on previous lookup")
	}

	if existing == nil {
		// nothing to do
		return nil
	}

	// we need the doc ID so we can clean up inverted indices currently
	// pointing to this object
	docID, updateTime, err := storobj.DocIDAndTimeFromBinary(existing)
	if err != nil {
		return errors.Wrap(err, "get existing doc id from object binary")
	}

	if deletionTime.IsZero() {
		err = bucket.Delete(idBytes)
	} else {
		err = bucket.DeleteWith(idBytes, deletionTime)
	}
	if err != nil {
		return errors.Wrap(err, "delete object from bucket")
	}

	err = s.cleanupInvertedIndexOnDelete(existing, docID)
	if err != nil {
		return errors.Wrap(err, "delete object from bucket")
	}

	if s.hasTargetVectors() {
		for targetVector, queue := range s.queues {
			if err = queue.Delete(docID); err != nil {
				return fmt.Errorf("delete from vector index queue of vector %q: %w", targetVector, err)
			}
		}
	} else {
		if err = s.queue.Delete(docID); err != nil {
			return errors.Wrap(err, "delete from vector index queue")
		}
	}

	if err = s.mayDeleteObjectHashTree(idBytes, updateTime, deletionTime); err != nil {
		return errors.Wrap(err, "object deletion in hashtree")
	}

	return nil
}

func (b *deleteObjectsBatcher) flushWALs(ctx context.Context) {
	before := time.Now()
	defer b.shard.Metrics().BatchDelete(before, "shard_flush_wals")

	if err := b.shard.Store().WriteWALs(); err != nil {
		for i := range b.objects {
			b.setErrorAtIndex(err, i)
		}
	}

	if b.shard.hasTargetVectors() {
		for targetVector, vectorIndex := range b.shard.VectorIndexes() {
			if err := vectorIndex.Flush(); err != nil {
				for i := range b.objects {
					b.setErrorAtIndex(fmt.Errorf("target vector %s: %w", targetVector, err), i)
				}
			}
		}
	} else {
		if err := b.shard.VectorIndex().Flush(); err != nil {
			for i := range b.objects {
				b.setErrorAtIndex(err, i)
			}
		}
	}

	if err := b.shard.GetPropertyLengthTracker().Flush(); err != nil {
		for i := range b.objects {
			b.setErrorAtIndex(err, i)
		}
	}
}

func (b *deleteObjectsBatcher) setErrorAtIndex(err error, index int) {
	b.Lock()
	defer b.Unlock()
	b.objects[index].Err = err
}

func (s *Shard) findDocIDs(ctx context.Context, filters *filters.LocalFilter) ([]uint64, error) {
	allowList, err := inverted.NewSearcher(s.index.logger, s.store, s.index.getSchema.ReadOnlyClass,
		nil, s.index.classSearcher, s.index.stopwords, s.versioner.version, s.isFallbackToSearchable,
		s.tenant(), s.index.Config.QueryNestedRefLimit, s.bitmapFactory).
		DocIDs(ctx, filters, additional.Properties{}, s.index.Config.ClassName)
	if err != nil {
		return nil, err
	}
	return allowList.Slice(), nil
}

func (s *Shard) FindUUIDs(ctx context.Context, filters *filters.LocalFilter) ([]strfmt.UUID, error) {
	docs, err := s.findDocIDs(ctx, filters)
	if err != nil {
		return nil, err
	}

	var (
		uuids   = make([]strfmt.UUID, len(docs))
		currIdx = 0
	)

	for _, doc := range docs {
		uuid, err := s.uuidFromDocID(doc)
		if err != nil {
			// TODO: More than likely this will occur due to an object which has already been deleted.
			//       However, this is not a guarantee. This can be improved by logging, or handling
			//       errors other than `id not found` rather than skipping them entirely.
			s.index.logger.WithField("op", "shard.find_uuids").WithField("docID", doc).WithError(err).Debug("failed to find UUID for docID")
			continue
		}
		uuids[currIdx] = uuid
		currIdx++
	}
	return uuids[:currIdx], nil
}
