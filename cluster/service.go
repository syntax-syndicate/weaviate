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

package cluster

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/bootstrap"
	"github.com/weaviate/weaviate/cluster/resolver"
	"github.com/weaviate/weaviate/cluster/rpc"
	"github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
)

// Service class serves as the primary entry point for the Raft layer, managing and coordinating
// the key functionalities of the distributed consensus protocol.
type Service struct {
	*Raft

	raftAddr string
	config   *Config

	rpcClient *rpc.Client
	rpcServer *rpc.Server
	logger    *logrus.Logger

	// closing channels
	closeBootstrapper chan struct{}
	closeWaitForDB    chan struct{}
}

// New returns a Service configured with cfg. The service will initialize internals gRPC api & clients to other cluster
// nodes.
// Raft store will be initialized and ready to be started. To start the service call Open().
func New(selector cluster.NodeSelector, cfg Config) *Service {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.RPCPort)
	cl := rpc.NewClient(resolver.NewRpc(cfg.IsLocalHost, cfg.RPCPort), cfg.RaftRPCMessageMaxSize, cfg.SentryEnabled, cfg.Logger)
	fsm := NewFSM(cfg)
	raft := NewRaft(selector, &fsm, cl)
	return &Service{
		Raft:              raft,
		raftAddr:          fmt.Sprintf("%s:%d", cfg.Host, cfg.RaftPort),
		config:            &cfg,
		rpcClient:         cl,
		rpcServer:         rpc.NewServer(&fsm, raft, addr, cfg.Logger, cfg.RaftRPCMessageMaxSize, cfg.SentryEnabled),
		logger:            cfg.Logger,
		closeBootstrapper: make(chan struct{}),
		closeWaitForDB:    make(chan struct{}),
	}
}

// Open internal RPC service to handle node communication,
// bootstrap the Raft node, and restore the database state
func (c *Service) Open(ctx context.Context, db schema.Indexer) error {
	c.logger.WithField("servers", c.config.NodeNameToPortMap).Info("open cluster service")
	if err := c.rpcServer.Open(); err != nil {
		return fmt.Errorf("start rpc service: %w", err)
	}

	if err := c.Raft.store.init(); err != nil {
		return fmt.Errorf("initialize raft store: %w", err)
	}

	if err := c.Raft.Open(ctx, db); err != nil {
		return fmt.Errorf("open raft store: %w", err)
	}

	hasState, err := raft.HasExistingState(c.Raft.store.logCache, c.Raft.store.logStore, c.Raft.store.snapshotStore)
	if err != nil {
		return err
	}

	c.log.WithField("hasState", hasState).Info("raft init")

	bs := bootstrap.NewBootstrapper(
		c.rpcClient,
		c.config.NodeID,
		c.raftAddr,
		c.config.NodeToAddressResolver,
		c.Raft.Ready,
	)

	bCtx, bCancel := context.WithTimeout(ctx, c.config.BootstrapTimeout)
	defer bCancel()

	if hasState {
		currRAFTCfg, err := raft.GetConfiguration(c.store.raftConfig(), c.store, c.store.logCache, c.store.logStore, c.store.snapshotStore, c.store.raftTransport)
		if err != nil {
			return err
		}

		for _, s := range currRAFTCfg.Servers {
			hostname := c.store.addrResolver.NodeIPToHostname(strings.Split(string(s.Address), ":")[0])
			addr := c.store.addrResolver.NodeAddress(string(s.ID))
			if hostname == "" || addr != strings.Split(string(s.Address), ":")[0] {
				servers := bs.Servers([]string{}, c.config.NodeNameToPortMap)
				if leader, err := bs.Remove(ctx, servers, string(s.ID)); err != nil {
					c.log.WithFields(logrus.Fields{
						"servers": servers,
						"action":  "removed cluster",
					}).WithError(err).Warning("failed to remove cluster, will notify next if voter")
				} else {
					c.log.WithFields(logrus.Fields{
						"action": "removing",
						"leader": leader,
						"server": s,
					}).Info("successfully removed node")
				}

				if leader, err := bs.JoinNode(ctx, servers, string(s.ID), string(s.Address), c.config.Voter); err != nil {
					c.log.WithFields(logrus.Fields{
						"servers": servers,
						"action":  "joining exiting cluster",
					}).WithError(err).Warning("failed to join cluster, will notify next if voter")
				} else {
					c.log.WithFields(logrus.Fields{
						"action":  "joining",
						"servers": servers,
						"server":  s,
						"leader":  leader,
					}).Info("successfully joined cluster")
				}
			}
		}
	}

	if !hasState {
		// don't bootstrap at all if there was already a state
		if err := bs.Do(
			bCtx,
			c.config.NodeNameToPortMap,
			c.logger,
			c.config.Voter, c.closeBootstrapper); err != nil {
			return fmt.Errorf("bootstrap: %w", err)
		}
	}

	if err := c.WaitUntilDBRestored(ctx, 10*time.Second, c.closeWaitForDB); err != nil {
		return fmt.Errorf("restore database: %w", err)
	}

	return nil
}

// Close closes the raft service and frees all allocated ressources. Internal RAFT store will be closed and if
// leadership is assumed it will be transferred to another node. gRPC server and clients will also be closed.
func (c *Service) Close(ctx context.Context) error {
	enterrors.GoWrapper(func() {
		c.closeBootstrapper <- struct{}{}
		c.closeWaitForDB <- struct{}{}
	}, c.logger)

	c.logger.Info("closing raft FSM store ...")
	if err := c.Raft.Close(ctx); err != nil {
		return err
	}

	c.logger.Info("closing raft-rpc client ...")
	c.rpcClient.Close()

	c.logger.Info("closing raft-rpc server ...")
	c.rpcServer.Close()
	return nil
}

// Ready returns or not whether the node is ready to accept requests.
func (c *Service) Ready() bool {
	return c.Raft.Ready()
}

// LeaderWithID is used to return the current leader address and ID of the cluster.
// It may return empty strings if there is no current leader or the leader is unknown.
func (c *Service) LeaderWithID() (string, string) {
	return c.Raft.LeaderWithID()
}

func (c *Service) StorageCandidates() []string {
	return c.Raft.StorageCandidates()
}
