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

package rangeable

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestRangeable(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	cleanup := func() {
		err := client.Schema().AllDeleter().Do(context.Background())
		require.Nil(t, err)
	}
	cleanup()
	defer cleanup()

	vTrue := true
	vFalse := false
	class := &models.Class{
		Class:      "BenchmarkRangeableClass",
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:              "int_filterable",
				DataType:          schema.DataTypeInt.PropString(),
				IndexFilterable:   &vTrue,
				IndexRangeFilters: &vFalse,
			},
			{
				Name:              "int_rangeable",
				DataType:          schema.DataTypeInt.PropString(),
				IndexFilterable:   &vFalse,
				IndexRangeFilters: &vTrue,
			},
			{
				Name:              "int_migrate",
				DataType:          schema.DataTypeInt.PropString(),
				IndexFilterable:   &vTrue,
				IndexRangeFilters: &vFalse,
			},
			{
				Name:              "number_filterable",
				DataType:          schema.DataTypeNumber.PropString(),
				IndexFilterable:   &vTrue,
				IndexRangeFilters: &vFalse,
			},
			{
				Name:              "number_rangeable",
				DataType:          schema.DataTypeNumber.PropString(),
				IndexFilterable:   &vFalse,
				IndexRangeFilters: &vTrue,
			},
			{
				Name:              "number_migrate",
				DataType:          schema.DataTypeNumber.PropString(),
				IndexFilterable:   &vTrue,
				IndexRangeFilters: &vFalse,
			},
			{
				Name:              "date_filterable",
				DataType:          schema.DataTypeDate.PropString(),
				IndexFilterable:   &vTrue,
				IndexRangeFilters: &vFalse,
			},
			{
				Name:              "date_rangeable",
				DataType:          schema.DataTypeDate.PropString(),
				IndexFilterable:   &vFalse,
				IndexRangeFilters: &vTrue,
			},
			{
				Name:              "date_migrate",
				DataType:          schema.DataTypeDate.PropString(),
				IndexFilterable:   &vTrue,
				IndexRangeFilters: &vFalse,
			},
		},
	}

	err = client.Schema().ClassCreator().
		WithClass(class).
		Do(context.Background())
	require.NoError(t, err)

	batches := 100
	perBatch := 100
	queries := 10
	queryLimit := 10_000

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	objects := make([]*models.Object, 0, perBatch)
	uuidStr := func(id int) strfmt.UUID {
		s := fmt.Sprintf("%032x", id)
		return strfmt.UUID(strings.Join([]string{
			s[:8], s[8:12], s[12:16], s[16:20], s[20:],
		}, "-"))
	}

	for batch := 0; batch < batches; batch++ {
		objects = objects[:0]

		for i := 0; i < perBatch; i++ {
			id := batch*perBatch + i + 1
			max := id * 10
			i64max := int64(max)
			f64max := float64(max)

			// fmt.Printf("id [%v]\n", id)
			// fmt.Printf("max [%v]\n", max)
			// fmt.Printf("i64max [%v]\n", i64max)
			// fmt.Printf("f64max [%v]\n", f64max)

			randInt := r.Int63n(i64max*2) - i64max
			randNumber := r.Float64()*f64max*2 - f64max
			randDate := time.Unix(0, 0).Add(time.Hour * time.Duration(r.Int63n(i64max)))

			// fmt.Printf("randInt [%v]\n", randInt)
			// fmt.Printf("randNumber [%v]\n", randNumber)
			// fmt.Printf("randDate [%v]\n", randDate)
			// fmt.Printf("uuid [%v]\n", uuidStr(id))

			objects = append(objects, &models.Object{
				Class: class.Class,
				ID:    uuidStr(id),
				Properties: map[string]interface{}{
					"int_filterable":    randInt,
					"int_rangeable":     randInt,
					"int_migrate":       randInt,
					"number_filterable": randNumber,
					"number_rangeable":  randNumber,
					"number_migrate":    randNumber,
					"date_filterable":   randDate,
					"date_rangeable":    randDate,
					"date_migrate":      randDate,
				},
			})
		}

		fmt.Printf("adding batch %d\n", batch)

		responses, err := client.Batch().ObjectsBatcher().
			WithObjects(objects...).
			Do(context.Background())
		require.NoError(t, err)
		require.Len(t, responses, perBatch)

		for _, response := range responses {
			s := response.Result.Status
			require.Equal(t, "SUCCESS", *s)
		}
	}

	totalMax := batches * perBatch * 10
	i64totalMax := int64(totalMax)
	f64totalMax := float64(totalMax)

	// fmt.Printf("totalMax [%+v]\n", totalMax)
	// fmt.Printf("i64totalMax [%+v]\n", i64totalMax)
	// fmt.Printf("f64totalMax [%+v]\n", f64totalMax)

	type supportedValueType interface {
		int64 | float64 | time.Time
	}

	type singleResult[T supportedValueType] struct {
		value      T
		operator   filters.WhereOperator
		filterable time.Duration
		rangeable  time.Duration
	}

	type result[T supportedValueType] struct {
		single          []singleResult[T]
		totalFilterable time.Duration
		totalRangeable  time.Duration
	}

	queryInt := func(t *testing.T, propName string, operator filters.WhereOperator, value int64) time.Duration {
		whereInt := filters.Where().
			WithPath([]string{propName}).
			WithOperator(operator).
			WithValueInt(value)

		builder := client.GraphQL().Get().
			WithClassName(class.Class).
			WithLimit(queryLimit).
			WithWhere(whereInt).
			WithFields(
				graphql.Field{Name: propName},
				graphql.Field{Name: "_additional", Fields: []graphql.Field{{Name: "id"}}},
			)

		start := time.Now()
		resp, err := builder.Do(context.Background())
		duration := time.Since(start)

		assert.NoError(t, err)
		assert.Empty(t, resp.Errors)

		return duration
	}

	queryIntsSingle := func(res *result[int64], pos int, operator filters.WhereOperator, value int64) {
		res.single[pos].value = value
		res.single[pos].operator = operator
		res.single[pos].filterable = queryInt(t, "int_filterable", operator, value)
		res.single[pos].rangeable = queryInt(t, "int_rangeable", operator, value)

		res.totalFilterable += res.single[pos].filterable
		res.totalRangeable += res.single[pos].rangeable
	}

	queryInts := func(res *result[int64], query int, value int64) {
		queryIntsSingle(res, 2*query, filters.LessThan, value)
		queryIntsSingle(res, 2*query+1, filters.GreaterThanEqual, value)
	}

	queryNumber := func(t *testing.T, propName string, operator filters.WhereOperator, value float64) time.Duration {
		whereNumber := filters.Where().
			WithPath([]string{propName}).
			WithOperator(operator).
			WithValueNumber(value)

		builder := client.GraphQL().Get().
			WithClassName(class.Class).
			WithLimit(queryLimit).
			WithWhere(whereNumber).
			WithFields(
				graphql.Field{Name: propName},
				graphql.Field{Name: "_additional", Fields: []graphql.Field{{Name: "id"}}},
			)

		start := time.Now()
		resp, err := builder.Do(context.Background())
		duration := time.Since(start)

		assert.NoError(t, err)
		assert.Empty(t, resp.Errors)

		return duration
	}

	queryNumbersSingle := func(res *result[float64], pos int, operator filters.WhereOperator, value float64) {
		res.single[pos].value = value
		res.single[pos].operator = operator
		res.single[pos].filterable = queryNumber(t, "number_filterable", operator, value)
		res.single[pos].rangeable = queryNumber(t, "number_rangeable", operator, value)

		res.totalFilterable += res.single[pos].filterable
		res.totalRangeable += res.single[pos].rangeable
	}

	queryNumbers := func(res *result[float64], query int, value float64) {
		queryNumbersSingle(res, 2*query, filters.LessThanEqual, value)
		queryNumbersSingle(res, 2*query+1, filters.GreaterThan, value)
	}

	queryDate := func(t *testing.T, propName string, operator filters.WhereOperator, value time.Time) time.Duration {
		whereDate := filters.Where().
			WithPath([]string{propName}).
			WithOperator(operator).
			WithValueDate(value)

		builder := client.GraphQL().Get().
			WithClassName(class.Class).
			WithLimit(queryLimit).
			WithWhere(whereDate).
			WithFields(
				graphql.Field{Name: propName},
				graphql.Field{Name: "_additional", Fields: []graphql.Field{{Name: "id"}}},
			)

		start := time.Now()
		resp, err := builder.Do(context.Background())
		duration := time.Since(start)

		assert.NoError(t, err)
		assert.Empty(t, resp.Errors)

		return duration
	}

	queryDatesSingle := func(res *result[time.Time], pos int, operator filters.WhereOperator, value time.Time) {
		res.single[pos].value = value
		res.single[pos].operator = operator
		res.single[pos].filterable = queryDate(t, "date_filterable", operator, value)
		res.single[pos].rangeable = queryDate(t, "date_rangeable", operator, value)

		res.totalFilterable += res.single[pos].filterable
		res.totalRangeable += res.single[pos].rangeable
	}

	queryDates := func(res *result[time.Time], query int, value time.Time) {
		queryDatesSingle(res, 2*query, filters.LessThan, value)
		queryDatesSingle(res, 2*query+1, filters.GreaterThan, value)
	}

	resultInts := &result[int64]{
		single:          make([]singleResult[int64], 2*queries),
		totalFilterable: 0,
		totalRangeable:  0,
	}
	resultNumbers := &result[float64]{
		single:          make([]singleResult[float64], 2*queries),
		totalFilterable: 0,
		totalRangeable:  0,
	}
	resultDates := &result[time.Time]{
		single:          make([]singleResult[time.Time], 2*queries),
		totalFilterable: 0,
		totalRangeable:  0,
	}
	for i := 0; i < queries; i++ {
		randInt := r.Int63n(i64totalMax*2) - i64totalMax
		randNumber := r.Float64()*f64totalMax*2 - f64totalMax
		randDate := time.Unix(0, 0).Add(time.Hour * time.Duration(r.Int63n(i64totalMax)))

		// fmt.Printf("randInt [%+v]\n", randInt)
		// fmt.Printf("randNumber [%+v]\n", randNumber)
		// fmt.Printf("randDate [%+v]\n\n", randDate)

		queryInts(resultInts, i, randInt)
		queryNumbers(resultNumbers, i, randNumber)
		queryDates(resultDates, i, randDate)
	}

	fmt.Printf("ints\nfilterable: %s\nrangeable: %s\n%+v\n\n",
		resultInts.totalFilterable.String(), resultInts.totalRangeable.String(), resultInts)
	fmt.Printf("numbers\nfilterable: %s\nrangeable: %s\n%+v\n\n",
		resultNumbers.totalFilterable.String(), resultNumbers.totalRangeable.String(), resultNumbers)
	fmt.Printf("dates\nfilterable: %s\nrangeable: %s\n%+v\n\n",
		resultDates.totalFilterable.String(), resultDates.totalRangeable.String(), resultDates)

	// compaction
	time.Sleep(50 * time.Second)

	resultIntsComp := &result[int64]{
		single:          make([]singleResult[int64], 2*queries),
		totalFilterable: 0,
		totalRangeable:  0,
	}
	resultNumbersComp := &result[float64]{
		single:          make([]singleResult[float64], 2*queries),
		totalFilterable: 0,
		totalRangeable:  0,
	}
	resultDatesComp := &result[time.Time]{
		single:          make([]singleResult[time.Time], 2*queries),
		totalFilterable: 0,
		totalRangeable:  0,
	}
	for i := 0; i < queries; i++ {
		randInt := r.Int63n(i64totalMax*2) - i64totalMax
		randNumber := r.Float64()*f64totalMax*2 - f64totalMax
		randDate := time.Unix(0, 0).Add(time.Hour * time.Duration(r.Int63n(i64totalMax)))

		// fmt.Printf("randInt [%+v]\n", randInt)
		// fmt.Printf("randNumber [%+v]\n", randNumber)
		// fmt.Printf("randDate [%+v]\n\n", randDate)

		queryInts(resultIntsComp, i, randInt)
		queryNumbers(resultNumbersComp, i, randNumber)
		queryDates(resultDatesComp, i, randDate)
	}

	fmt.Printf("ints compacted\nfilterable: %s\nrangeable: %s\n%+v\n\n",
		resultIntsComp.totalFilterable.String(), resultIntsComp.totalRangeable.String(), resultIntsComp)
	fmt.Printf("numbers compacted\nfilterable: %s\nrangeable: %s\n%+v\n\n",
		resultNumbersComp.totalFilterable.String(), resultNumbersComp.totalRangeable.String(), resultNumbersComp)
	fmt.Printf("dates compacted\nfilterable: %s\nrangeable: %s\n%+v\n\n",
		resultDatesComp.totalFilterable.String(), resultDatesComp.totalRangeable.String(), resultDatesComp)
}
