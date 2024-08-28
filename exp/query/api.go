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

	"github.com/sirupsen/logrus"
	v1 "github.com/weaviate/weaviate/adapters/handlers/grpc/v1"
	"github.com/weaviate/weaviate/adapters/repos/db"
	clusterapi "github.com/weaviate/weaviate/cluster/proto/api"
	clusterschema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/traverser"

	"google.golang.org/protobuf/proto"
)

// API is the core query API that is transport agnostic (http, grpc, etc).
type API struct {
	log    logrus.FieldLogger
	config *config.Config

	// svc provides the underlying search API via v1.WeaviateServer
	// TODO(kavi): Split `v1.WeaviateServer` into composable `v1.Searcher` and everything else.
	svc protocol.WeaviateServer

	dbRepo *db.DB
}

func NewAPI(
	dbRepo *db.DB,
	traverser *traverser.Traverser,
	authComposer composer.TokenFunc,
	allowAnonymousAccess bool,
	schemaManager *schema.Manager,
	batchManager *objects.BatchManager,
	config *config.Config,
	log logrus.FieldLogger,
) *API {
	return &API{
		log:    log,
		config: config,
		svc:    v1.NewService(traverser, authComposer, allowAnonymousAccess, schemaManager, batchManager, config, log),
		dbRepo: dbRepo,
	}
}

func (api *API) ensureClassTenant(className, tenantName string) error {
	log := &logrus.Logger{}
	fmt.Println("ensureClassTenant")
	remoteSchemaManager, err := RemoteSchemaManager(
		SchemaConfig{
			Host: "localhost",
			Port: 50051,
		},
		log,
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("copy sharding state")
	shardingState := remoteSchemaManager.CopyShardingState(className)
	fmt.Println("get class")
	class, err := remoteSchemaManager.GetClass(context.TODO(), &models.Principal{}, className)
	fmt.Println("get class done", err)
	if err != nil {
		log.Fatalf("failed to get class from remote schema manager: %v", err)
	}
	subCommandReq := clusterapi.AddClassRequest{Class: class, State: shardingState}
	subCommand, err := json.Marshal(&subCommandReq)
	if err != nil {
		log.Fatalf("marshal request: %v", err)
	}
	ar := clusterapi.ApplyRequest{
		Type:       clusterapi.ApplyRequest_TYPE_ADD_CLASS,
		Class:      className,
		SubCommand: subCommand,
	}
	// migrator := db.NewMigrator(api.dbRepo, api.log)
	// e := schema.NewExecutor(migrator, remoteSchemaManager, api.log, func(string) error { return nil })
	localSchemaManager := &clusterschema.SchemaManager{} //clusterSchema.NewSchemaManager("query", e, schema.NewParser(nil, vectorindex.ParseAndValidateConfig, nil), log)
	err = localSchemaManager.AddClass(&ar, "query", false)
	if err != nil {
		log.Fatalf("failed to add class: %v", err)
	}
	addTenantsReq := clusterapi.AddTenantsRequest{ClusterNodes: []string{"metadata-0"}, Tenants: []*clusterapi.Tenant{{Name: tenantName}}}
	subCommand, err = proto.Marshal(&addTenantsReq)
	if err != nil {
		log.Fatalf("marshal request: %v", err)
	}
	ar = clusterapi.ApplyRequest{
		Type:       clusterapi.ApplyRequest_TYPE_ADD_TENANT,
		Class:      className,
		SubCommand: subCommand,
	}
	err = localSchemaManager.AddTenants(&ar, false) // v?
	if err != nil {
		log.Fatalf("failed to add tenant: %v", err)
	}

	// s3o := objectstorage.NewS3ObjectStorage()
	// err = s3o.DownloadTenant(objectstorageIndexedPrefix, className, tenantName, "/serverless/data")
	// if err != nil {
	// 	panic(err)
	// }
	return nil
}
