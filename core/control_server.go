package core

import (
	"context"
	"log"
	"sync"
	"time"

	chat "github.com/Serial-Experiments-Weebify/weebchat/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ControlServer struct {
	chat.UnimplementedControlPlaneServer

	Address   string
	SecretKey string

	nodeMu      sync.RWMutex
	nextNodeIdx int64
	head        *DataNode
	tail        *DataNode
	nodes       []*DataNode
}

type DataNode struct {
	Info   *chat.NodeInfo
	Conn   *grpc.ClientConn
	Client *chat.MessageBoardClient
}

func NewControlServer(
	address string,
	secretKey string,
) *ControlServer {
	return &ControlServer{
		Address:   address,
		SecretKey: secretKey,
		nodes:     make([]*DataNode, 0),
	}
}

func nodeInfo(n *DataNode) *chat.NodeInfo {
	if n == nil {
		return nil
	}
	return n.Info
}

func (s *ControlServer) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*chat.GetClusterStateResponse, error) {
	s.nodeMu.RLock()
	defer s.nodeMu.RUnlock()

	log.Printf("GetClusterState called - head: %v, tail: %v", s.head, s.tail)

	return &chat.GetClusterStateResponse{
		Head: nodeInfo(s.head),
		Tail: nodeInfo(s.tail),
	}, nil
}

const SecretKeyHeader = "x-secret-key"

func createNode(info *chat.NodeInfo) (*DataNode, error) {
	log.Printf("NewClient %s\n", info.Address)
	conn, err := grpc.NewClient(info.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Printf("Failed... %v\n", err)
		return nil, err
	}

	log.Printf("MessageBoardClient %s\n", info.Address)
	client := chat.NewMessageBoardClient(conn)

	log.Printf("Pinging... %s\n", info.Address)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err = client.Ping(ctx, &emptypb.Empty{})

	if err != nil {
		log.Printf("Failed... %v\n", err)
		return nil, err
	}

	return &DataNode{
		Info:   info,
		Conn:   conn,
		Client: &client,
	}, nil
}

func (s *ControlServer) JoinCluster(ctx context.Context, req *chat.JoinClusterRequest) (*chat.JoinClusterResponse, error) {
	// Validate secret key from metadata
	if err := s.validateSecretKey(ctx); err != nil {
		return nil, err
	}

	if req.GetSelf() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing self node info")
	}

	s.nodeMu.Lock()
	defer s.nodeMu.Unlock()

	log.Printf("JoinCluster called - self: %s", req.GetSelf())

	s.nextNodeIdx++
	node, err := createNode(&chat.NodeInfo{
		NodeId:  s.nextNodeIdx,
		Address: req.GetSelf(),
	})

	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create node client: %v", err)
	}

	s.nodes = append(s.nodes, node)

	cTail := s.tail

	if len(s.nodes) == 1 {
		s.head = node
		s.tail = node
		log.Printf("First node joined - setting as head and tail")
	} else {
		s.tail = node
		log.Printf("Node joined - setting as new tail")
	}

	if cTail != nil {
		tPredIdx := len(s.nodes) - 2
		var tPred *DataNode

		if tPredIdx >= 0 {
			tPred = s.nodes[tPredIdx]
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		ctx = metadata.AppendToOutgoingContext(ctx, SecretKeyHeader, s.SecretKey)
		defer cancel()
		_, err := (*cTail.Client).ReconfigureNode(ctx, &chat.NodeConfiguration{
			Predecessor: tPred.Info,
			Successor:   s.tail.Info,
		})

		if err != nil {
			// remove the node we just added
			s.nodes = s.nodes[:len(s.nodes)-1]
			s.tail = cTail
			return nil, status.Errorf(codes.Internal, "failed to reconfigure old tail: %v", err)
		}
	}

	return &chat.JoinClusterResponse{
		You:   node.Info.NodeId,
		Front: nodeInfo(cTail),
		Back:  nil,
	}, nil
}

// validateSecretKey checks if the request contains a valid secret key in metadata
func (s *ControlServer) validateSecretKey(ctx context.Context) error {
	if s.SecretKey == "" {
		return nil // No secret key configured, allow all
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}

	keys := md.Get(SecretKeyHeader)
	if len(keys) == 0 {
		return status.Error(codes.Unauthenticated, "missing secret key")
	}

	if keys[0] != s.SecretKey {
		return status.Error(codes.Unauthenticated, "invalid secret key")
	}

	return nil
}

func StartControlServer(address string, secretKey string) *grpc.Server {
	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)
	controlServer := NewControlServer(address, secretKey)
	chat.RegisterControlPlaneServer(grpcServer, controlServer)

	return grpcServer
}
