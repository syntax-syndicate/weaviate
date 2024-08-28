package query

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/rpc"
	clusterschema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	schemaconfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/scaler"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	shardingconfig "github.com/weaviate/weaviate/usecases/sharding/config"
	"google.golang.org/protobuf/proto"
)

// RemoteSchemaManager returns a schema manager that works with a remote raft cluster instead of assuming a schema must
// be available locally
func RemoteSchemaManager(cfg SchemaConfig, logger *logrus.Logger) (*schema.Manager, error) {
	apiConfig := config.Config{}
	authorizer := authorization.New(apiConfig)
	modulesProvider := modules.NewProvider(logger)
	scaler := &Scaler{}
	remoteAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	log := &logrus.Logger{} // TODO replace with real logger
	return schema.NewManager(
		&Validator{},
		remoteSchemaManager{
			schemaAddr: remoteAddr,
			cl:         rpc.NewClient(&RpcAddressSolver{Addr: remoteAddr}, 1024*1024*1024, false, log),
		},
		NewRemoteSchemaReader(cfg),
		nil, // Nil schema repo as this is for legacy schema operations which weaviate serverless doesn't do
		logger,
		authorizer,
		apiConfig,
		vectorindex.ParseAndValidateConfig,
		modulesProvider,
		inverted.ValidateConfig,
		modulesProvider,
		RemoteClusterState{},
		scaler, // Fake scaler as this is not handled by weaviate serverless
		nil,    // TODO offload module?
	)
}

type remoteSchemaManager struct {
	schemaAddr string
	cl         *rpc.Client
}

func NewRemoteSchemaManager(cfg SchemaConfig) *remoteSchemaManager {
	// TODO: For PoC sake we always connect to the same metadata done, we fake the address resolver to always return the
	// same address. In the future this should hit a load balancer
	remoteAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	log := &logrus.Logger{}
	return &remoteSchemaManager{
		schemaAddr: remoteAddr,
		cl:         rpc.NewClient(&RpcAddressSolver{Addr: remoteAddr}, 1024*1024*1024, false, log),
	}
}

// Schema writes operation.
func (r remoteSchemaManager) AddClass(ctx context.Context, cls *models.Class, ss *sharding.State) (uint64, error) {
	if cls == nil || cls.Class == "" {
		return 0, fmt.Errorf("nil class or empty class name")
	}

	req := api.AddClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_ADD_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	leader := "metadata-0"
	resp, err := r.cl.Apply(context.TODO(), leader, command)
	if err != nil {
		return 0, err
	}

	return resp.Version, err
}

func (r remoteSchemaManager) RestoreClass(ctx context.Context, cls *models.Class, ss *sharding.State) (uint64, error) {
	panic("not implemented yet")
}

func (r remoteSchemaManager) UpdateClass(ctx context.Context, cls *models.Class, ss *sharding.State) (uint64, error) {
	panic("not implemented yet")
}

func (r remoteSchemaManager) DeleteClass(ctx context.Context, name string) (uint64, error) {
	panic("not implemented yet")
}

func (r remoteSchemaManager) AddProperty(ctx context.Context, class string, p ...*models.Property) (uint64, error) {
	panic("not implemented yet")
}

func (r remoteSchemaManager) UpdateShardStatus(ctx context.Context, class, shard, status string) (uint64, error) {
	panic("not implemented yet")
}

func (r remoteSchemaManager) AddTenants(ctx context.Context, class string, req *api.AddTenantsRequest) (uint64, error) {
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_ADD_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	leader := "metadata-0"
	resp, err := r.cl.Apply(context.TODO(), leader, command)
	if err != nil {
		return 0, err
	}

	return resp.Version, err
}

func (r remoteSchemaManager) UpdateTenants(ctx context.Context, class string, req *api.UpdateTenantsRequest) (uint64, error) {
	panic("not implemented yet")
}

func (r remoteSchemaManager) DeleteTenants(ctx context.Context, class string, req *api.DeleteTenantsRequest) (uint64, error) {
	panic("not implemented yet")
}

// Cluster related operations
func (r remoteSchemaManager) Join(_ context.Context, nodeID, raftAddr string, voter bool) error {
	panic("not implemented yet")
}

func (r remoteSchemaManager) Remove(_ context.Context, nodeID string) error {
	panic("not implemented yet")
}

func (r remoteSchemaManager) Stats() map[string]any {
	panic("not implemented yet")
}

func (r remoteSchemaManager) StoreSchemaV1() error {
	panic("not implemented yet")
}

func (r remoteSchemaManager) QueryReadOnlyClasses(classes ...string) (map[string]versioned.Class, error) {
	if len(classes) == 0 {
		return nil, fmt.Errorf("empty classes names")
	}
	slices.Sort(classes)
	classes = slices.Compact(classes)
	if len(classes) == 0 {
		return map[string]versioned.Class{}, fmt.Errorf("empty classes names")
	}
	if len(classes) > 1 && classes[0] == "" {
		classes = classes[1:]
	}
	req := api.QueryReadOnlyClassesRequest{Classes: classes}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return map[string]versioned.Class{}, fmt.Errorf("marshal request: %w", err)
	}
	command := &api.QueryRequest{
		Type:       api.QueryRequest_TYPE_GET_CLASSES,
		SubCommand: subCommand,
	}
	queryResp, err := r.cl.Query(context.Background(), "metadata-0", command)
	if err != nil {
		return map[string]versioned.Class{}, fmt.Errorf("failed to execute query: %w", err)
	}
	if len(queryResp.Payload) == 0 {
		return nil, nil
	}
	resp := api.QueryReadOnlyClassResponse{}
	err = json.Unmarshal(queryResp.Payload, &resp)
	if err != nil {
		return map[string]versioned.Class{}, fmt.Errorf("failed to unmarshal query result: %w", err)
	}
	return resp.Classes, nil
}

func (r remoteSchemaManager) QuerySchema() (models.Schema, error) {
	panic("not implemented yet")
}

func (r remoteSchemaManager) QueryTenants(class string, tenants []string) ([]*models.Tenant, uint64, error) {
	panic("not implemented yet")
}

func (r remoteSchemaManager) QueryShardOwner(class, shard string) (string, uint64, error) {
	return "indexer", 0, nil
}

func (r remoteSchemaManager) QueryTenantsShards(class string, tenants ...string) (map[string]string, uint64, error) {
	req := api.QueryTenantsShardsRequest{Class: class, Tenants: tenants}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &api.QueryRequest{
		Type:       api.QueryRequest_TYPE_GET_TENANTS_SHARDS,
		SubCommand: subCommand,
	}
	queryResp, err := r.cl.Query(context.Background(), "metadata-0", command)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to execute query: %w", err)
	}
	resp := api.QueryTenantsShardsResponse{}
	err = json.Unmarshal(queryResp.Payload, &resp)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return resp.TenantsActivityStatus, resp.SchemaVersion, nil
}

func (r remoteSchemaManager) QueryShardingState(class string) (*sharding.State, uint64, error) {
	panic("not implemented yet")
}

func (r remoteSchemaManager) StorageCandidates() []string {
	return []string{}
}

type SchemaConfig struct {
	Host string `koanf:"host"`
	Port int    `koanf:"port"`
}

type Scaler struct{}

func (s *Scaler) SetSchemaReader(sr scaler.SchemaReader) {
}

func (s *Scaler) Scale(ctx context.Context, className string, updated shardingconfig.Config, prevReplFactor, newReplFactor int64) (*sharding.State, error) {
	panic("This should not happen in weaviate serverless")
}

type Validator struct{}

func (m *Validator) ValidateVectorIndexConfigUpdate(
	old, updated schemaconfig.VectorIndexConfig,
) error {
	// hnsw is the only supported vector index type at the moment, so no need
	// to check, we can always use that an hnsw-specific validation should be
	// used for now.
	switch old.IndexType() {
	case vectorindex.VectorIndexTypeHNSW:
		return hnsw.ValidateUserConfigUpdate(old, updated)
	case vectorindex.VectorIndexTypeFLAT:
		return flat.ValidateUserConfigUpdate(old, updated)
	case vectorindex.VectorIndexTypeDYNAMIC:
		return dynamic.ValidateUserConfigUpdate(old, updated)
	}
	return fmt.Errorf("Invalid index type: %s", old.IndexType())
}

func (m *Validator) ValidateVectorIndexConfigsUpdate(old, updated map[string]schemaconfig.VectorIndexConfig,
) error {
	for vecName := range old {
		if err := m.ValidateVectorIndexConfigUpdate(old[vecName], updated[vecName]); err != nil {
			return fmt.Errorf("vector %q", vecName)
		}
	}
	return nil
}

func (m *Validator) ValidateInvertedIndexConfigUpdate(old, updated *models.InvertedIndexConfig,
) error {
	return inverted.ValidateUserConfigUpdate(old, updated)
}

type RpcAddressSolver struct {
	Addr string
}

func (r RpcAddressSolver) Address(raftAddress string) (string, error) {
	return r.Addr, nil
}

type remoteSchemaReader struct {
	schemaAddr string
	cl         *rpc.Client
}

func NewRemoteSchemaReader(cfg SchemaConfig) *remoteSchemaReader {
	// TODO: For PoC sake we always connect to the same metadata done, we fake the address resolver to always return the
	// same address. In the future this should hit a load balancer
	remoteAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	log := &logrus.Logger{} // TODO replace with real logger
	return &remoteSchemaReader{
		schemaAddr: remoteAddr,
		cl:         rpc.NewClient(&RpcAddressSolver{Addr: remoteAddr}, 1024*1024*1024, false, log),
	}
}

func (r *remoteSchemaReader) WaitForUpdate(ctx context.Context, version uint64) error {
	return nil
}

func (r *remoteSchemaReader) ClassEqual(name string) string {
	panic("not implemented yet")
}

func (r *remoteSchemaReader) MultiTenancy(class string) models.MultiTenancyConfig {
	panic("not implemented yet")
}

func (r *remoteSchemaReader) ClassInfo(class string) (ci clusterschema.ClassInfo) {
	panic("not implemented yet")
}

func (r *remoteSchemaReader) ReadOnlyClass(name string) *models.Class {
	// Build the query and execute it
	req := api.QueryReadOnlyClassesRequest{Classes: []string{name}}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return &models.Class{}
	}
	command := &api.QueryRequest{
		Type:       api.QueryRequest_TYPE_GET_CLASSES,
		SubCommand: subCommand,
	}
	queryResp, err := r.Query(context.Background(), command)
	if err != nil {
		return &models.Class{}
	}

	// Empty payload doesn't unmarshal to an empty struct and will instead result in an error.
	// We have an empty payload when the requested class if not present in the schema.
	// In that case return a nil pointer and no error.
	if len(queryResp.Payload) == 0 {
		return &models.Class{}
	}

	// Unmarshal the response
	resp := api.QueryReadOnlyClassResponse{}
	err = json.Unmarshal(queryResp.Payload, &resp)
	if err != nil {
		return &models.Class{}
	}
	return resp.Classes[name].Class
}

func (r *remoteSchemaReader) ReadOnlySchema() models.Schema {
	// TODO if this is implemented, it breaks the dbRepo.WaitForStartup call when a class exists on the metadata nodes
	return models.Schema{}
	// command := &api.QueryRequest{
	// 	Type: api.QueryRequest_TYPE_GET_SCHEMA,
	// }
	// queryResp, err := r.Query(context.Background(), command)
	// if err != nil {
	// 	return models.Schema{}
	// }

	// // Unmarshal the response
	// resp := api.QuerySchemaResponse{}
	// err = json.Unmarshal(queryResp.Payload, &resp)
	// if err != nil {
	// 	return models.Schema{}
	// }
	// for _, class := range resp.Schema.Classes {
	// 	vic, err := vectorindex.ParseAndValidateConfig(class.VectorIndexConfig, class.VectorIndexType)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	class.VectorIndexConfig = vic
	// }
	// return resp.Schema
}

func (r *remoteSchemaReader) CopyShardingState(class string) *sharding.State {
	req := api.QueryShardingStateRequest{Class: class}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		panic(err)
	}
	command := &api.QueryRequest{
		Type:       api.QueryRequest_TYPE_GET_SHARDING_STATE,
		SubCommand: subCommand,
	}
	queryResp, err := r.Query(context.Background(), command)
	if err != nil {
		panic(err)
	}

	resp := api.QueryShardingStateResponse{}
	err = json.Unmarshal(queryResp.Payload, &resp)
	if err != nil {
		panic(err)
	}

	return resp.State
}

func (r *remoteSchemaReader) ShardReplicas(class, shard string) ([]string, error) {
	return []string{"indexer"}, nil
}

func (r *remoteSchemaReader) ShardFromUUID(class string, uuid []byte) string {
	panic("not implemented yet")
}

func (r *remoteSchemaReader) ShardOwner(class, shard string) (string, error) {
	panic("not implemented yet")
}

func (r *remoteSchemaReader) Read(class string, reader func(*models.Class, *sharding.State) error) error {
	c := r.ReadOnlyClass(class)
	if c == nil {
		panic("class not found")
	}
	s := r.CopyShardingState(class)
	if s == nil {
		panic("sharding state not found")
	}
	return reader(c, s)
}

func (r *remoteSchemaReader) GetShardsStatus(class, tenant string) (models.ShardStatusList, error) {
	panic("not implemented yet")
}

func (r *remoteSchemaReader) ClassInfoWithVersion(ctx context.Context, class string, version uint64) (clusterschema.ClassInfo, error) {
	panic("not implemented yet")
}

func (r *remoteSchemaReader) MultiTenancyWithVersion(ctx context.Context, class string, version uint64) (models.MultiTenancyConfig, error) {
	panic("not implemented yet")
}

func (r *remoteSchemaReader) ReadOnlyClassWithVersion(ctx context.Context, class string, version uint64) (*models.Class, error) {
	panic("not implemented yet")
}

func (r *remoteSchemaReader) ShardOwnerWithVersion(ctx context.Context, lass, shard string, version uint64) (string, error) {
	panic("not implemented yet")
}

func (r *remoteSchemaReader) ShardFromUUIDWithVersion(ctx context.Context, class string, uuid []byte, version uint64) (string, error) {
	panic("not implemented yet")
}

func (r *remoteSchemaReader) ShardReplicasWithVersion(ctx context.Context, class, shard string, version uint64) ([]string, error) {
	panic("not implemented yet")
}

func (r *remoteSchemaReader) TenantsShardsWithVersion(ctx context.Context, version uint64, class string, tenants ...string) (map[string]string, error) {
	panic("not implemented yet")
}

func (r *remoteSchemaReader) CopyShardingStateWithVersion(ctx context.Context, class string, version uint64) (*sharding.State, error) {
	panic("not implemented yet")
}

// Query receives a QueryRequest and ensure it is executed on the leader and returns the related QueryResponse
// If any error happens it returns it
func (r *remoteSchemaReader) Query(ctx context.Context, req *api.QueryRequest) (*api.QueryResponse, error) {
	return r.cl.Query(ctx, r.schemaAddr, req)
}

type RemoteClusterState struct{}

// SortCandidates implements schema.clusterState.
func (r RemoteClusterState) SortCandidates(nodes []string) []string {
	panic("unimplemented")
}

// StorageCandidates implements schema.clusterState.
func (r RemoteClusterState) StorageCandidates() []string {
	panic("unimplemented")
}

func (r RemoteClusterState) Hostnames() []string {
	panic("not implemented")
}

func (r RemoteClusterState) AllNames() []string {
	panic("not implemented")
}

func (r RemoteClusterState) Candidates() []string {
	return []string{"indexer"}
}

func (r RemoteClusterState) LocalName() string {
	return "indexer"
}

func (r RemoteClusterState) NodeCount() int {
	return 1
}

func (r RemoteClusterState) NodeHostname(nodeName string) (string, bool) {
	return "query:12345", true
}

func (r RemoteClusterState) ClusterHealthScore() int {
	panic("not implemented")
}

func (r RemoteClusterState) SchemaSyncIgnored() bool {
	panic("not implemented")
}

func (r RemoteClusterState) SkipSchemaRepair() bool {
	panic("not implemented")
}

func (r RemoteClusterState) AllHostnames() []string {
	panic("not implemented")
}

func (r RemoteClusterState) NonStorageNodes() []string {
	panic("not implemented")
}
