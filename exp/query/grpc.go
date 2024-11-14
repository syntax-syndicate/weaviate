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

package query

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	v1 "github.com/weaviate/weaviate/adapters/handlers/grpc/v1"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	grpc "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// GRPC transport on top of the query.API.
type GRPC struct {
	api *API
	log logrus.FieldLogger

	// Needed to extrat `filters` from the payload.
	schema SchemaQuerier

	// TODO(kavi): This should go away once we split v1.WeaviateServer into composable v1.Searcher
	grpc.UnimplementedWeaviateServer

	// parser decodes the search request
	parser *v1.Parser

	// encoder takes care of encoding the search responses
	encoder *v1.Replier
}

func NewGRPC(api *API, schema SchemaQuerier, log logrus.FieldLogger) *GRPC {
	return &GRPC{
		api:    api,
		log:    log,
		schema: schema,
	}
}

func (g *GRPC) Search(ctx context.Context, req *grpc.SearchRequest) (*grpc.SearchReply, error) {
	class, err := g.schema.Collection(ctx, req.Collection)
	if err != nil {
		return nil, err
	}

	getClass := func(name string) *models.Class {
		return class
	}

	parsed, err := requestFromProto(req, getClass)
	if err != nil {
		return nil, err
	}
	parsed.Class = class

	res, err := g.api.Search(ctx, parsed)
	if err != nil {
		return nil, err
	}

	return g.encode(res), nil
}

func (g *GRPC) encodeResponse(res *SearchResponse, start time.Time) *grpc.SearchReply {
	var resp grpc.SearchReply

	objs := make([]interface{}, 0)

	for _, r := range res.Results {
		objs = append(objs, r.Obj.Object.Properties)
	}

	g.encoder.Search(objs, start)
}

func (g *GRPC) decodeRequest(req *grpc.SearchRequest) (*SearchRequest, error) {
}

func requestFromProto(req *grpc.SearchRequest, getClass func(string) *models.Class) (*SearchRequest, error) {
	sr := &SearchRequest{
		Collection: req.Collection,
		Tenant:     req.Tenant,
		Limit:      int(req.Limit),
	}
	if req.NearText != nil {
		sr.NearText = req.NearText.Query
		if req.NearText.Certainty != nil {
			sr.Certainty = *req.NearText.Certainty
		}
	}
	if req.Filters != nil {
		filter, err := v1.ExtractFilters(req.Filters, getClass, req.Collection)
		if err != nil {
			return nil, err
		}
		sr.Filters = &filters.LocalFilter{Root: &filter}
	}
	return sr, nil
}

func toProtoResponse(res *SearchResponse) *grpc.SearchReply {
	var resp grpc.SearchReply

	// TODO(kavi): copy rest of the fields accordingly.
	for _, v := range res.Results {
		props := grpc.Properties{
			Fields: make(map[string]*grpc.Value),
		}
		objprops := v.Obj.Object.Properties.(map[string]interface{})
		for prop, val := range objprops {
			props.Fields[prop] = &grpc.Value{
				Kind: &grpc.Value_StringValue{
					StringValue: val.(string),
				},
			}
		}

		resp.Results = append(resp.Results, &grpc.SearchResult{
			Metadata: &grpc.MetadataResult{
				Id:        v.Obj.ID().String(),
				Certainty: float32(v.Certainty),
			},
			Properties: &grpc.PropertiesResult{
				TargetCollection: v.Obj.Object.Class,
				NonRefProps:      &props,
			},
		})

	}
	return &resp
}
