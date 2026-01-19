package core

import (
	"context"
	"log"
	"slices"
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
	topics      []int64
}

type DataNode struct {
	Info   *chat.NodeInfo
	Conn   *grpc.ClientConn
	Client *chat.MessageBoardClient
	Topics []int64
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
	conn, err := grpc.NewClient(info.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Printf("Client failed... %v\n", err)
		return nil, err
	}

	client := chat.NewMessageBoardClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err = client.Ping(ctx, &emptypb.Empty{})

	if err != nil {
		log.Printf("Ping failed... %v\n", err)
		return nil, err
	}

	return &DataNode{
		Info:   info,
		Conn:   conn,
		Client: &client,
	}, nil
}

func spreadTopics(topics *[]int64, nodes *[]*DataNode) {
	for node := range *nodes {
		(*nodes)[node].Topics = make([]int64, 0)
	}

	if len(*nodes) < 3 {
		return
	}

	nSubNodes := len(*nodes) - 2

	for i, topic := range *topics {
		targetNode := (i % nSubNodes) + 1
		(*nodes)[targetNode].Topics = append((*nodes)[targetNode].Topics, topic)
	}
}
func (s *ControlServer) nodeDead(node *DataNode) {
	s.nodeMu.Lock()
	defer s.nodeMu.Unlock()
	s.nodeDeadNOLOCK(node)
}

func (s *ControlServer) nodeDeadNOLOCK(node *DataNode) {
badnode:
	for {
		log.Printf("Node dead: %v", node.Info)

		var idx int = -1

		for i, n := range s.nodes {
			if n.Info.NodeId == node.Info.NodeId {
				idx = i
				break
			}
		}

		if idx == -1 {
			log.Print("Node not found in list!")
			return
		}

		if len(s.nodes) == 0 {
			log.Print("No nodes left in cluster!")
		}

		if idx == 0 {
			s.nodes = s.nodes[1:]
		} else if idx == len(s.nodes)-1 {
			s.nodes = s.nodes[:len(s.nodes)-1]
		} else {
			s.nodes = append(s.nodes[:idx], s.nodes[idx+1:]...)
		}

		if len(s.nodes) == 0 {
			s.head = nil
			s.tail = nil
		} else {
			s.head = s.nodes[0]
			s.tail = s.nodes[len(s.nodes)-1]
		}

		// reconfigure all nodes
		spreadTopics(&s.topics, &s.nodes)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		ctx = metadata.AppendToOutgoingContext(ctx, SecretKeyHeader, s.SecretKey)
		defer cancel()

		for i, n := range s.nodes {
			var pred *DataNode = nil
			var succ *DataNode = nil

			if i > 0 {
				pred = s.nodes[i-1]
			}

			if i < len(s.nodes)-1 {
				succ = s.nodes[i+1]
			}

			_, err := (*n.Client).ReconfigureNode(ctx, &chat.NodeConfiguration{
				Predecessor: nodeInfo(pred),
				Successor:   nodeInfo(succ),
				TopicIds:    n.Topics,
			})

			if err != nil {
				node = n
				continue badnode
			}
		}

		// all ok
		break
	}
}

func (s *ControlServer) pingRoutine(node *DataNode) {
	log.Printf("Watching health of %v", node.Info)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		ctx = metadata.AppendToOutgoingContext(ctx, SecretKeyHeader, s.SecretKey)
		_, err := (*node.Client).Ping(ctx, &emptypb.Empty{})

		if err != nil {
			log.Printf("Node ping %v: Error: %v", node.Info, err)
			s.nodeDead(node)
			cancel()
			return
		}
		cancel()

		time.Sleep(5 * time.Second)
	}
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

	log.Printf("JoinCluster: %s", req.GetSelf())

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

	go s.pingRoutine(node)

	return &chat.JoinClusterResponse{
		You:   node.Info.NodeId,
		Front: nodeInfo(cTail),
		Back:  nil,
	}, nil
}

func nodeWithLeastTopics(arr *[]*DataNode) (*DataNode, int64) {
	if len(*arr) == 0 {
		return nil, -1
	}

	minNode := (*arr)[1]
	minTopics := len(minNode.Topics)
	idx := int64(1)

	for i := range len(*arr) - 2 {
		node := (*arr)[i+1]
		if len(node.Topics) < minTopics {
			minNode = node
			minTopics = len(node.Topics)
			idx = int64(i + 1)
		}
	}

	return minNode, int64(idx)
}

func (s *ControlServer) RequestSubscriptionNode(ctx context.Context, req *chat.SubRequest) (*chat.SubscriptionNodeResponse, error) {
	if err := s.validateSecretKey(ctx); err != nil {
		return nil, err
	}

	s.nodeMu.Lock()
	defer s.nodeMu.Unlock()

	aware := slices.Contains(s.topics, req.GetTopicId())

	if !aware {
		s.topics = append(s.topics, req.GetTopicId())
		// new topic, find a node to serve it

		if len(s.nodes) < 3 {
			return nil, status.Error(codes.NotFound, "subscriptions not available")
		}

		targetNode, idx := nodeWithLeastTopics(&s.nodes)

		targetNode.Topics = append(targetNode.Topics, req.GetTopicId())

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		ctx = metadata.AppendToOutgoingContext(ctx, SecretKeyHeader, s.SecretKey)
		defer cancel()

		before := s.nodes[idx-1]
		after := s.nodes[idx+1]

		_, err := (*targetNode.Client).ReconfigureNode(ctx, &chat.NodeConfiguration{
			Predecessor: nodeInfo(before),
			Successor:   nodeInfo(after),
			TopicIds:    targetNode.Topics,
		})

		if err == nil {
			return &chat.SubscriptionNodeResponse{
				Node:           targetNode.Info,
				SubscribeToken: "<3",
			}, nil
		}

		s.nodeDeadNOLOCK(targetNode)
	}

	// we are now aware of the topic, find a node that serves it

	for _, node := range s.nodes {
		if slices.Contains(node.Topics, req.GetTopicId()) {
			return &chat.SubscriptionNodeResponse{
				Node:           node.Info,
				SubscribeToken: "<3",
			}, nil
		}
	}

	return nil, status.Error(codes.NotFound, "subscriptions not available")
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
