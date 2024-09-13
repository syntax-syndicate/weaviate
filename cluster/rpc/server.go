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

package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_sentry "github.com/johnbellone/grpc-middleware-sentry"
	"github.com/sirupsen/logrus"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/cluster/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type raftPeers interface {
	Join(id string, addr string, voter bool) error
	Notify(id string, addr string) error
	Remove(id string) error
	Leader() string
}

type raftFSM interface {
	Execute(ctx context.Context, cmd *cmd.ApplyRequest) (uint64, error)
	Query(ctx context.Context, req *cmd.QueryRequest) (*cmd.QueryResponse, error)
}

type Server struct {
	raftPeers          raftPeers
	raftFSM            raftFSM
	listenAddress      string
	grpcMessageMaxSize int
	log                *logrus.Logger
	sentryEnabled      bool

	grpcServer         *grpc.Server
	querierSubscribers *utils.QuerierSubscribers
}

// NewServer returns the Server implementing the RPC interface for RAFT peers management and execute/query commands.
// The server must subsequently be started with Open().
// The server will be configure the gRPC service with grpcMessageMaxSize.
func NewServer(raftPeers raftPeers, raftFSM raftFSM, listenAddress string, log *logrus.Logger, grpcMessageMaxSize int, sentryEnabled bool) *Server {
	return &Server{
		raftPeers:          raftPeers,
		raftFSM:            raftFSM,
		listenAddress:      listenAddress,
		log:                log,
		grpcMessageMaxSize: grpcMessageMaxSize,
		sentryEnabled:      sentryEnabled,
	}
}

func NewServerV2(raftPeers raftPeers, raftFSM raftFSM, listenAddress string, log *logrus.Logger, grpcMessageMaxSize int, sentryEnabled bool, querierSubscribers *utils.QuerierSubscribers) *Server {
	s := NewServer(raftPeers, raftFSM, listenAddress, log, grpcMessageMaxSize, sentryEnabled)
	s.querierSubscribers = querierSubscribers
	return s
}

// JoinPeer will notify the RAFT cluster that a new peer is joining the cluster.
// Returns an error and the current raft leader if joining fails.
func (s *Server) JoinPeer(_ context.Context, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error) {
	err := s.raftPeers.Join(req.Id, req.Address, req.Voter)
	if err != nil {
		return &cmd.JoinPeerResponse{Leader: s.raftPeers.Leader()}, toRPCError(err)
	}

	return &cmd.JoinPeerResponse{}, nil
}

// RemovePeer will notify the RAFT cluster that a peer is removed from the cluster.
// Returns an error and the current raft leader if removal fails.
func (s *Server) RemovePeer(_ context.Context, req *cmd.RemovePeerRequest) (*cmd.RemovePeerResponse, error) {
	err := s.raftPeers.Remove(req.Id)
	if err != nil {
		return &cmd.RemovePeerResponse{Leader: s.raftPeers.Leader()}, toRPCError(err)
	}
	return &cmd.RemovePeerResponse{}, nil
}

// NotifyPeer will notify the RAFT cluster that a peer has notified that it is ready to be joined.
// Returns an error if notifying fails.
func (s *Server) NotifyPeer(_ context.Context, req *cmd.NotifyPeerRequest) (*cmd.NotifyPeerResponse, error) {
	return &cmd.NotifyPeerResponse{}, toRPCError(s.raftPeers.Notify(req.Id, req.Address))
}

// Apply will update the RAFT FSM representation to apply req.
// Returns the FSM version of that change.
// Returns an error and the current raft leader if applying fails.
func (s *Server) Apply(ctx context.Context, req *cmd.ApplyRequest) (*cmd.ApplyResponse, error) {
	v, err := s.raftFSM.Execute(ctx, req)
	if err != nil {
		return &cmd.ApplyResponse{Leader: s.raftPeers.Leader()}, toRPCError(err)
	}
	return &cmd.ApplyResponse{Version: v}, nil
}

// Query will read the RAFT FSM schema representation using req.
// Returns the result of the query.
// Returns an error if querying fails.
func (s *Server) Query(ctx context.Context, req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	resp, err := s.raftFSM.Query(ctx, req)
	if err != nil {
		return &cmd.QueryResponse{}, toRPCError(err)
	}

	return resp, nil
}

func (s *Server) QuerierSubscription(stream cmd.ClusterService_QuerierSubscriptionServer) error {
	// TODO handle panics/return val
	// TODO 10 buffer?
	querierSubscription := make(chan utils.ClassTenantDataVersion, 10)
	s.querierSubscribers.Subscribe(querierSubscription)

	wg := sync.WaitGroup{}
	wg.Add(2)

	streamClosed := make(chan struct{})

	// process events from querier
	go func() {
		defer wg.Done()
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				fmt.Println("s.qs EOF")
				streamClosed <- struct{}{}
				return
			}
			if err != nil {
				fmt.Println(err)
				return
			}
			// TODO stream.context?
			switch in.Type {
			case cmd.QuerierEvent_UNSPECIFIED:
				panic(fmt.Errorf("querier event type is unspecified"))
			case cmd.QuerierEvent_TENANT_DATA_VERSION_READY:
				s.querierSubscribers.AddNodeTenantDataVersion(utils.ClassTenantDataVersion{
					ClassName:         in.ClassName,
					TenantName:        in.TenantName,
					TenantDataVersion: in.TenantDataVersion,
				})
			case cmd.QuerierEvent_TENANT_DATA_VERSION_EVICTED:
				s.querierSubscribers.RemoveNodeTenantDataVersion(utils.ClassTenantDataVersion{
					ClassName:         in.ClassName,
					TenantName:        in.TenantName,
					TenantDataVersion: in.TenantDataVersion,
				})
			case cmd.QuerierEvent_SHUTTING_DOWN:
				panic(fmt.Errorf("querier notified us it's shutting down, TODO handle"))
			}
		}
	}()

	// send events to querier
	// NOTE currently sending all events to all querier nodes, no filtering
	go func() {
		defer wg.Done()
		for {
			// TODO think more about this select
			select {
			case <-streamClosed:
				fmt.Println("stream closed")
				close(streamClosed)
				s.querierSubscribers.Unsubscribe(querierSubscription)
				return
			// case <-stream.Context().Done():
			// 	fmt.Println("context done")
			// 	s.querierSubscribers.Unsubscribe(querierSubscription)
			// 	return
			case ctdv := <-querierSubscription:
				err := stream.Send(&cmd.ServerEvent{
					Type:              cmd.ServerEvent_NEW_TENANT_DATA_VERSION,
					Class:             ctdv.ClassName,
					Tenant:            ctdv.TenantName,
					TenantDataVersion: ctdv.TenantDataVersion,
				})
				if err != nil {
					panic(err)
				}
			}
		}
	}()

	wg.Wait()

	close(querierSubscription)
	return nil
}

// Leader returns the current leader of the RAFT cluster.
func (s *Server) Leader() string {
	return s.raftPeers.Leader()
}

// Open starts the server and registers it as the cluster service server.
// Returns asynchronously once the server has started.
// Returns an error if the configured listenAddress is invalid.
// Returns an error if the configured listenAddress is un-usable to listen on.
func (s *Server) Open() error {
	s.log.WithField("address", s.listenAddress).Info("starting cloud rpc server ...")
	if s.listenAddress == "" {
		return fmt.Errorf("address of rpc server cannot be empty")
	}

	listener, err := net.Listen("tcp", s.listenAddress)
	if err != nil {
		return fmt.Errorf("server tcp net.listen: %w", err)
	}

	var options []grpc.ServerOption
	options = append(options, grpc.MaxRecvMsgSize(s.grpcMessageMaxSize))
	if s.sentryEnabled {
		options = append(options,
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
				grpc_sentry.UnaryServerInterceptor(),
			)))
	}
	s.grpcServer = grpc.NewServer(options...)
	cmd.RegisterClusterServiceServer(s.grpcServer, s)
	enterrors.GoWrapper(func() {
		if err := s.grpcServer.Serve(listener); err != nil {
			s.log.WithError(err).Error("serving incoming requests")
			panic("error accepting incoming requests")
		}
	}, s.log)
	return nil
}

// Close closes the server and free any used ressources.
func (s *Server) Close() {
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
}

// toRPCError returns a gRPC error with the right error code based on the error.
func toRPCError(err error) error {
	if err == nil {
		return nil
	}

	var ec codes.Code
	switch {
	case errors.Is(err, types.ErrNotLeader), errors.Is(err, types.ErrLeaderNotFound):
		ec = codes.ResourceExhausted
	case errors.Is(err, types.ErrNotOpen):
		ec = codes.Unavailable
	default:
		ec = codes.Internal
	}
	return status.Error(ec, err.Error())
}
