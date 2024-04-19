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
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
)

type propValuePair struct {
	prop     string
	operator filters.Operator

	// set for all values that can be served by an inverted index, i.e. anything
	// that's not a geoRange
	_value []byte

	// only set if operator=OperatorWithinGeoRange, as that cannot be served by a
	// byte value from an inverted index
	valueGeoRange      *filters.GeoRange
	docIDs             docBitmap
	children           []*propValuePair
	hasFilterableIndex bool
	hasSearchableIndex bool
	Class              *models.Class // The schema
	logger             logrus.FieldLogger
}

func newPropValuePair(class *models.Class, logger logrus.FieldLogger) (*propValuePair, error) {
	if class == nil {
		return nil, errors.Errorf("class must not be nil")
	}
	return &propValuePair{logger: logger, docIDs: newDocBitmap(), Class: class}, nil
}

func (pv *propValuePair) SetValue(value []byte) {
	pv._value = value
}

func (pv *propValuePair) Value() []byte {
	// copy pv.value so other code can't modify it
	if pv._value == nil {
		return nil
	}
	value := make([]byte, len(pv._value))
	copy(value, pv._value)
	return value
}

func (pv *propValuePair) DocIds() []uint64 {
	return pv.docIDs.IDs()
}

func (pv *propValuePair) fetchDocIDs_unmerged(s *Searcher, limit int) error {
	if pv.operator.OnValue() {
		var bucketName string
		if pv.hasFilterableIndex {
			bucketName = helpers.BucketFromPropertyNameLSM(pv.prop)
		} else if pv.hasSearchableIndex {
			bucketName = helpers.BucketSearchableFromPropertyNameLSM(pv.prop)
		} else {
			return errors.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
		}

		b := s.store.Bucket(bucketName)

		// TODO text_rbm_inverted_index find better way check whether prop len
		if b == nil && strings.HasSuffix(bucketName, filters.InternalPropertyLength) {
			return errors.Errorf("Property length must be indexed to be filterable! " +
				"add `IndexPropertyLength: true` to the invertedIndexConfig." +
				"Geo-coordinates, phone numbers and data blobs are not supported by property length.")
		}

		if b == nil && pv.operator == filters.OperatorIsNull {
			return errors.Errorf("Nullstate must be indexed to be filterable! " +
				"add `indexNullState: true` to the invertedIndexConfig")
		}

		if b == nil && (pv.prop == filters.InternalPropCreationTimeUnix ||
			pv.prop == filters.InternalPropLastUpdateTimeUnix) {
			return errors.Errorf("timestamps must be indexed to be filterable! " +
				"add `indexTimestamps: true` to the invertedIndexConfig")
		}

		if b == nil && pv.operator != filters.OperatorWithinGeoRange {
			// a nil bucket is ok for a WithinGeoRange filter, as this query is not
			// served by the inverted index, but propagated to a secondary index in
			// .docPointers()
			return errors.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
		}

		ctx := context.TODO() // TODO: pass through instead of spawning new
		dbm, err := s.docBitmap(ctx, []byte{}, b, limit, pv)
		if err != nil {
			return err
		}
		pv.docIDs = dbm
	} else {
		eg := enterrors.NewErrorGroupWrapper(pv.logger)
		// prevent unbounded concurrency, see
		// https://github.com/weaviate/weaviate/issues/3179 for details
		eg.SetLimit(2 * _NUMCPU)
		for i, child := range pv.children {
			i, child := i, child
			eg.Go(func() error {
				// Explicitly set the limit to 0 (=unlimited) as this is a nested filter,
				// otherwise we run into situations where each subfilter on their own
				// runs into the limit, possibly yielding in "less than limit" results
				// after merging.
				err := child.fetchDocIDs(s, 0)
				if err != nil {
					return errors.Wrapf(err, "nested child %d", i)
				}

				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return fmt.Errorf("nested query: %w", err)
		}
	}

	return nil
}

func (pv *propValuePair) fetchDocIDs(s *Searcher, limit int) error {
	if !lsmkv.FeatureUseMergedBuckets {
		return pv.fetchDocIDs_unmerged(s, limit)
	}
	if pv.operator.OnValue() {

		// TODO text_rbm_inverted_index find better way check whether prop len
		if strings.HasSuffix(pv.prop, filters.InternalPropertyLength) &&
			!pv.Class.InvertedIndexConfig.IndexPropertyLength {
			return errors.Errorf("Property length must be indexed to be filterable! add `IndexPropertyLength: true` to the invertedIndexConfig in %v.  Geo-coordinates, phone numbers and data blobs are not supported by property length.", pv.Class.Class)
		}

		if pv.operator == filters.OperatorIsNull && !pv.Class.InvertedIndexConfig.IndexNullState {
			return errors.Errorf("Nullstate must be indexed to be filterable! Add `indexNullState: true` to the invertedIndexConfig")
		}

		if (pv.prop == filters.InternalPropCreationTimeUnix ||
			pv.prop == filters.InternalPropLastUpdateTimeUnix) &&
			!pv.Class.InvertedIndexConfig.IndexTimestamps {
			return errors.Errorf("Timestamps must be indexed to be filterable! Add `IndexTimestamps: true` to the InvertedIndexConfig in %v", pv.Class.Class)
		}

		var mergedName string
		if pv.hasFilterableIndex {
			mergedName = "filterable_properties"
		} else if pv.hasSearchableIndex {
			mergedName = "searchable_properties"
		} else {
			return errors.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
		}

		var bucketName string
		if pv.hasFilterableIndex {
			bucketName = helpers.BucketFromPropertyNameLSM(pv.prop)
		} else if pv.hasSearchableIndex {
			bucketName = helpers.BucketSearchableFromPropertyNameLSM(pv.prop)
		} else {
			return errors.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
		}

		// TODO text_rbm_inverted_index find better way check whether prop len
		if strings.HasSuffix(pv.prop, filters.InternalPropertyLength) &&
			!pv.Class.InvertedIndexConfig.IndexPropertyLength {
			return errors.Errorf("Property length must be indexed to be filterable! add `IndexPropertyLength: true` to the invertedIndexConfig. Geo-coordinates, phone numbers and data blobs are not supported by property length.")
		}

		// TODO how to check if prop is phone number or data blob?

		if pv.operator == filters.OperatorIsNull {
			if !pv.Class.InvertedIndexConfig.IndexNullState {
				return errors.Errorf("Nullstate must be indexed to be filterable! add `indexNullState: true` to the invertedIndexConfig")
			}
		}

		if pv.prop == filters.InternalPropCreationTimeUnix || pv.prop == filters.InternalPropLastUpdateTimeUnix {
			if !pv.Class.InvertedIndexConfig.IndexTimestamps {
				return errors.Errorf("timestamps must be indexed to be filterable! add `indexTimestamps: true` to the invertedIndexConfig")
			}
		}

		bproxy, err := lsmkv.FetchMeABucket(s.store, mergedName, bucketName, pv.prop, s.propIds) // "" - old file is handled in _old
		if err != nil {
			return err
		}

		ctx := context.TODO() // TODO: pass through instead of spawning new
		dbm, err := s.docBitmap(ctx, []byte(pv.prop), bproxy, limit, pv)
		if err != nil {
			return err
		}
		pv.docIDs = dbm
	} else {
		for i, child := range pv.children {
			i, child := i, child
			// Explicitly set the limit to 0 (=unlimited) as this is a nested filter,
			// otherwise we run into situations where each subfilter on their own
			// runs into the limit, possibly yielding in "less than limit" results
			// after merging.
			err := child.fetchDocIDs(s, 0)
			if err != nil {
				return errors.Wrapf(err, "nested child %d", i)
			}

		}
	}

	return nil
}

func (pv *propValuePair) mergeDocIDs() (*docBitmap, error) {
	if pv.operator.OnValue() {
		return &pv.docIDs, nil
	}

	if pv.operator != filters.OperatorAnd && pv.operator != filters.OperatorOr {
		return nil, fmt.Errorf("unsupported operator: %s", pv.operator.Name())
	}
	if len(pv.children) == 0 {
		return nil, fmt.Errorf("no children for operator: %s", pv.operator.Name())
	}

	dbms := make([]*docBitmap, len(pv.children))
	for i, child := range pv.children {
		dbm, err := child.mergeDocIDs()
		if err != nil {
			return nil, errors.Wrapf(err, "retrieve doc bitmap of child %d", i)
		}
		dbms[i] = dbm
	}

	mergeRes := dbms[0].DocIDs.Clone()
	mergeFn := mergeRes.And
	if pv.operator == filters.OperatorOr {
		mergeFn = mergeRes.Or
	}

	for i := 1; i < len(dbms); i++ {
		mergeFn(dbms[i].DocIDs)
	}

	return &docBitmap{
		DocIDs: roaringset.Condense(mergeRes),
	}, nil
}
