// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"net"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/snapshot/api"
	snapshotstore "github.com/xataio/pgstream/pkg/snapshot/store"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type GRPCServer struct {
	api.UnimplementedSnapshotAPIServer
	store   snapshotstore.Store
	adapter apiAdapter
	logger  loglib.Logger
	conn    net.Listener
}

type Option func(s *GRPCServer)

func NewGRPCServer(cfg *Config, store snapshotstore.Store, opts ...Option) (*GRPCServer, error) {
	conn, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.port()))
	if err != nil {
		return nil, fmt.Errorf("error listening on configured port %d: %w", cfg.port(), err)
	}

	s := &GRPCServer{
		adapter: &adapter{},
		store:   store,
		logger:  loglib.NewNoopLogger(),
		conn:    conn,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(s *GRPCServer) {
		s.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "snapshot_grpc_server",
		})
	}
}

func (s *GRPCServer) Start() error {
	s.logger.Info("starting grpc snapshot server")

	grpcServer := grpc.NewServer()
	api.RegisterSnapshotAPIServer(grpcServer, s)

	s.logger.Info(fmt.Sprintf("listening on %v", s.conn.Addr()))
	return grpcServer.Serve(s.conn)
}

func (s *GRPCServer) Stop() error {
	s.logger.Info("stopping grpc snapshot server")
	return s.conn.Close()
}

func (s *GRPCServer) RequestSnapshot(ctx context.Context, apiReq *api.SnapshotRequest) (*emptypb.Empty, error) {
	req := s.adapter.toSnapshot(apiReq)
	if err := s.store.CreateSnapshotRequest(ctx, req); err != nil {
		s.logger.Error(err, "requesting snapshot")
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
