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
	"errors"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/proto/api"
	modsloads3 "github.com/weaviate/weaviate/modules/offload-s3"
)

const (
	TenantOffLoadingStatus = "FROZEN"

	// NOTE(kavi): using fixed nodeName that offloaded tenant under that prefix on object storage.
	// TODO(kavi): Make it dynamic.
	nodeName = "weaviate-0"
)

var ErrInvalidTenant = errors.New("invalid tenant status")

// API is the core query API that is transport agnostic (http, grpc, etc).
type API struct {
	log    logrus.FieldLogger
	config *Config

	tenant TenantInfo
	// svc provides the underlying search API via v1.WeaviateServer
	// TODO(kavi): Split `v1.WeaviateServer` into composable `v1.Searcher` and everything else.
	// svc    protocol.WeaviateServer
	// schema *schema.Manager
	// batch  *objects.BatchManager
	offload *modsloads3.Module
	onceFoo func()
}

type TenantInfo interface {
	TenantStatus(ctx context.Context, collection, tenant string) (string, error)
	StartQuerierSubscription(ctx context.Context, schemaGRPCHost string, schemaGRPCPort int) error
	SendTenantDataVersionEvents(e *api.QuerierEvent)
}

func NewAPI(
	tenant TenantInfo,
	offload *modsloads3.Module,
	config *Config,
	log logrus.FieldLogger,
) *API {
	return &API{
		log:     log,
		config:  config,
		tenant:  tenant,
		offload: offload,
		onceFoo: sync.OnceFunc(
			func() {
				err := tenant.StartQuerierSubscription(context.TODO(), config.SchemaGRPCHost, config.SchemaGRPCPort)
				if err != nil {
					panic(err)
				}
			},
		),
	}
}

// Search serves vector search over the offloaded tenant on object storage.
func (a *API) Search(ctx context.Context, req *SearchRequest) (*SearchResponse, error) {
	info, err := a.tenant.TenantStatus(ctx, req.Collection, req.Tenant)
	if err != nil {
		return nil, err
	}

	if info != TenantOffLoadingStatus {
		return nil, fmt.Errorf("tenant %q is not offloaded, %w", req.Tenant, ErrInvalidTenant)
	}
	fmt.Println("a.s of")
	go a.onceFoo()

	// src - s3://<collection>/<tenant>/<node>/
	// dst (local) - <data-path/<collection>/<tenant>
	fmt.Println("a.s aod")
	if err := a.offload.Download(ctx, req.Collection, req.Tenant, nodeName); err != nil {
		fmt.Println("a.s aod err", err)
		return nil, err
	}
	fmt.Println("a.s aod done")
	fmt.Println("NOTE Downloaded within Search:", req.Collection, req.Tenant)

	// Expectations
	// 0. Check if tenant status == FROZEN
	// 1. Build local path `/<datapath>/<collection>/<tenant>` from search request
	// 2. If it doesn't exist, fetch from object store to same path as above
	// 3. If it doesn't exist on object store, return 404
	// 4. Once in the local disk, load the index
	// 5. Searve the search.

	a.tenant.SendTenantDataVersionEvents(&api.QuerierEvent{
		Type:              api.QuerierEvent_TENANT_DATA_VERSION_READY,
		ClassName:         req.Collection,
		TenantName:        req.Tenant,
		TenantDataVersion: 0,
	})
	fmt.Println("a.s atstdve done")
	fmt.Println("NOTE Notified metadata server that we've downloaded within Search", req.Collection, req.Tenant)

	return &SearchResponse{}, nil
}

type SearchRequest struct {
	Collection string
	Tenant     string
}

type SearchResponse struct {
	IDs []string
}
