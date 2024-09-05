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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// GRPC transport on top of the query.API.
type GRPC struct {
	api *API
	log logrus.FieldLogger

	// TODO(kavi): This should go away once we split v1.WeaviateServer into composable v1.Searcher
	protocol.UnimplementedWeaviateServer
}

func NewGRPC(api *API, log logrus.FieldLogger) *GRPC {
	return &GRPC{
		api: api,
		log: log,
	}
}

var reqCount int

func (g *GRPC) Search(ctx context.Context, req *protocol.SearchRequest) (*protocol.SearchReply, error) {
	if reqCount > 0 {
		err := g.api.schema.DeleteTenants(context.TODO(), &models.Principal{}, req.Collection, []string{req.Tenant})
		if err != nil {
			panic(err)
		}
		// err = g.api.schema.DeleteClass(context.TODO(), &models.Principal{}, req.Collection)
		// if err != nil {
		// 	panic(err)
		// }
	}
	reqCount++

	// TODO only create class if not exists/not changed? replace with go client and do not hardcode the url
	c, err := g.api.schema.GetClass(context.TODO(), &models.Principal{}, req.Collection)
	if err != nil {
		panic(err)
	}
	if c == nil {
		r, err := http.Get(fmt.Sprintf("http://localhost:8080/v1/schema/%s", req.Collection))
		if err != nil {
			panic(err)
		}
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		cc := models.Class{}
		json.Unmarshal(body, &cc)
		classToCreate := models.Class{
			Class:              cc.Class,
			MultiTenancyConfig: cc.MultiTenancyConfig,
			// TODO fill in the other relevant parts of the class here?
		}
		_, _, err = g.api.schema.AddClass(context.TODO(), &models.Principal{}, &classToCreate)
		if err != nil {
			panic(err)
		}
	}
	tt, err := g.api.schema.GetConsistentTenants(context.TODO(), &models.Principal{}, req.Collection, false, []string{req.Tenant})
	if err != nil {
		panic(err)
	}
	if len(tt) == 0 {
		_, err = g.api.schema.AddTenants(context.TODO(), &models.Principal{}, req.Collection, []*models.Tenant{{Name: req.Tenant, ActivityStatus: "ACTIVE"}})
		if err != nil {
			panic(err)
		}
	}

	// TODO download objects from s3 here?
	// get current timestamp
	err = exec.Command("rm", "-rf", "/Users/nate/Repos/weaviate/data-wquery-0/article/MyTenant/*").Run()
	if err != nil {
		panic(err)
	}
	err = exec.Command("cp", "-r", "/Users/nate/Repos/weaviate/data-weaviate-0/article/MyTenant/", "/Users/nate/Repos/weaviate/data-wquery-0/article/MyTenant/").Run()
	if err != nil {
		panic(err)
	}

	// err = g.api.dbRepo.WaitForStartup(context.TODO())
	// if err != nil {
	// 	panic(err)
	// }

	// res, err := g.api.Search(ctx, requestFromProto(req))
	// if err != nil {
	// 	return nil, err
	// }

	r, err := g.api.dbRepo.Search(context.TODO(), dto.GetParams{
		ClassName: req.Collection,
		Tenant:    req.Tenant,
		Pagination: &filters.Pagination{
			Limit: 25,
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("NATEE r", r)
	results := []*protocol.SearchResult{}
	for i := 0; i < len(r); i++ {
		results = append(results, &protocol.SearchResult{
			Metadata: &protocol.MetadataResult{
				Id: r[i].ID.String(),
			},
		})
	}
	return &protocol.SearchReply{Results: results}, nil

	// return g.api.svc.Search(context.TODO(), req)
	// reply, err := g.api.svc.Search(context.TODO(), req)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("NATEE reply", reply)
	// fmt.Println("NATEE replyresults", reply.Results)

	// return toProtoResponse(res), nil
}

// func requestFromProto(req *protocol.SearchRequest) *SearchRequest {
// 	return &SearchRequest{
// 		Collection: req.Collection,
// 		Tenant:     req.Tenant,
// 	}
// }

// func toProtoResponse(res *SearchResponse) *protocol.SearchReply {
// 	return &protocol.SearchReply{}
// 	// return &protocol.SearchReply{
// 	// 	Results: []*protocol.SearchResult{
// 	// 		{
// 	// 			Metadata: &protocol.MetadataResult{
// 	// 				Id: res.Objects[0].ID.String(),
// 	// 			},
// 	// 		},
// 	// 	},
// 	// }
// }
