package core

import (
	"context"
	"fmt"
	"log"
	"math"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

type DataServer struct {
	chat.UnimplementedMessageBoardServer

	Address   string
	Control   string
	SecretKey string

	// Control plane client
	controlConn   *grpc.ClientConn
	controlCtx    context.Context
	controlClient chat.ControlPlaneClient

	// Nodes
	nodeMu      sync.RWMutex
	front       *chat.NodeInfo
	frontClient chat.MessageBoardClient
	back        *chat.NodeInfo
	backClient  chat.MessageBoardClient

	// Data storage
	dataLock      sync.RWMutex
	nextUserID    int64
	nextTopicID   int64
	nextMessageID int64
	users         map[int64]*chat.User
	topics        map[int64]*chat.Topic
	messages      map[int64][]*chat.Message // topic_id -> messages

	// Node configuration
	config *chat.NodeConfiguration
}

func NewDataServer(address, control, secretKey string) *DataServer {
	return &DataServer{
		Address:   address,
		Control:   control,
		SecretKey: secretKey,
		users:     make(map[int64]*chat.User),
		topics:    make(map[int64]*chat.Topic),
		messages:  make(map[int64][]*chat.Message),
	}
}

func (s *DataServer) validateSecretKey(ctx context.Context) error {
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

func (s *DataServer) IsHead() bool {
	s.nodeMu.RLock()
	defer s.nodeMu.RUnlock()
	return s.front == nil
}

func (s *DataServer) IsTail() bool {
	s.nodeMu.RLock()
	defer s.nodeMu.RUnlock()
	return s.back == nil
}

func (s *DataServer) IsMiddle() bool {
	s.nodeMu.RLock()
	defer s.nodeMu.RUnlock()
	return s.front != nil && s.back != nil
}

func (s *DataServer) CreateUser(ctx context.Context, req *chat.CreateUserRequest) (*chat.User, error) {
	if !s.IsHead() {
		return nil, fmt.Errorf("wrong node")
	}

	if req.GetName() == "" {
		return nil, fmt.Errorf("missing user name")
	}

	s.dataLock.Lock()
	defer s.dataLock.Unlock()

	s.nextUserID++
	user := &chat.User{
		Id:   s.nextUserID,
		Name: req.GetName(),
	}
	s.users[user.Id] = user

	log.Printf("CreateUser: %v", user)

	// TODO: propagate

	return user, nil
}

func (s *DataServer) CreateTopic(ctx context.Context, req *chat.CreateTopicRequest) (*chat.Topic, error) {
	if !s.IsHead() {
		return nil, fmt.Errorf("wrong node")
	}

	if req.GetName() == "" {
		return nil, fmt.Errorf("missing topic name")
	}

	s.dataLock.Lock()
	defer s.dataLock.Unlock()

	s.nextTopicID++
	topic := &chat.Topic{
		Id:   s.nextTopicID,
		Name: req.GetName(),
	}
	s.topics[topic.Id] = topic
	s.messages[topic.Id] = make([]*chat.Message, 0)

	log.Printf("CreateTopic: %v", topic)

	// TODO: propagate
	return topic, nil
}

func (s *DataServer) PostMessage(ctx context.Context, req *chat.PostMessageRequest) (*chat.Message, error) {
	if !s.IsHead() {
		return nil, fmt.Errorf("wrong node")
	}

	if req.GetText() == "" {
		return nil, fmt.Errorf("missing message text")
	}

	if req.GetUserId() == 0 {
		return nil, fmt.Errorf("missing user id")
	}

	if req.GetTopicId() == 0 {
		return nil, fmt.Errorf("missing topic id")
	}

	s.dataLock.Lock()
	defer s.dataLock.Unlock()

	topicID := req.GetTopicId()
	if _, ok := s.topics[topicID]; !ok {
		log.Printf("PostMessage: topic %d not found", topicID)
		return nil, nil
	}

	if _, ok := s.users[req.GetUserId()]; !ok {
		log.Printf("PostMessage: user %d not found", req.GetUserId())
		return nil, nil
	}

	s.nextMessageID++
	msg := &chat.Message{
		Id:        s.nextMessageID,
		TopicId:   topicID,
		UserId:    req.GetUserId(),
		Text:      req.GetText(),
		CreatedAt: timestamppb.Now(),
		Likes:     0,
	}
	s.messages[topicID] = append(s.messages[topicID], msg)

	log.Printf("PostMessage: %v", msg)

	//TODO: propagate

	return msg, nil
}

func (s *DataServer) LikeMessage(ctx context.Context, req *chat.LikeMessageRequest) (*chat.Message, error) {
	if !s.IsHead() {
		return nil, fmt.Errorf("wrong node")
	}

	if req.GetMessageId() == 0 {
		return nil, fmt.Errorf("missing message id")
	}

	if req.GetTopicId() == 0 {
		return nil, fmt.Errorf("missing topic id")
	}

	if req.GetUserId() == 0 {
		return nil, fmt.Errorf("missing user id")
	}

	s.dataLock.Lock()
	defer s.dataLock.Unlock()

	topicID := req.GetTopicId()
	msgs, ok := s.messages[topicID]
	if !ok {
		log.Printf("LikeMessage: topic %d not found", topicID)
		return nil, nil
	}

	var msg *chat.Message

	for _, m := range msgs {
		if m.Id == req.GetMessageId() {
			m.Likes++
			log.Printf("LikeMessage: message %d now has %d likes", m.Id, m.Likes)

			msg = m
		}
	}

	if msg == nil {
		return nil, fmt.Errorf("message not found")
	}

	// TODO: propagate

	return msg, nil
}

func (s *DataServer) GetSubscriptionNode(ctx context.Context, req *chat.SubscriptionNodeRequest) (*chat.SubscriptionNodeResponse, error) {
	s.dataLock.RLock()
	defer s.dataLock.RUnlock()

	// Dummy: return self as subscription node
	return &chat.SubscriptionNodeResponse{
		SubscribeToken: "dummy-token",
		Node: &chat.NodeInfo{
			NodeId:  1,
			Address: s.Address,
		},
	}, nil
}

func (s *DataServer) ListTopics(ctx context.Context, _ *emptypb.Empty) (*chat.ListTopicsResponse, error) {
	if !s.IsHead() {
		return nil, fmt.Errorf("wrong node")
	}

	s.dataLock.RLock()
	defer s.dataLock.RUnlock()

	topics := make([]*chat.Topic, 0, len(s.topics))
	for _, t := range s.topics {
		topics = append(topics, t)
	}

	log.Printf("ListTopics: returning %d topics", len(s.topics))
	return &chat.ListTopicsResponse{Topics: topics}, nil
}

func (s *DataServer) GetMessages(ctx context.Context, req *chat.GetMessagesRequest) (*chat.GetMessagesResponse, error) {
	if !s.IsHead() {
		return nil, fmt.Errorf("wrong node")
	}

	s.dataLock.RLock()
	defer s.dataLock.RUnlock()

	topicID := req.GetTopicId()
	msgs, ok := s.messages[topicID]
	if !ok {
		return &chat.GetMessagesResponse{Messages: []*chat.Message{}}, nil
	}

	// Apply from_message_id filter
	fromID := req.GetFromMessageId()
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = math.MaxInt32
	}

	result := make([]*chat.Message, 0)
	for _, msg := range msgs {
		if msg.Id > fromID {
			result = append(result, msg)
			if len(result) >= limit {
				break
			}
		}
	}

	log.Printf("GetMessages: returning %d messages for topic %d", len(result), topicID)
	return &chat.GetMessagesResponse{Messages: result}, nil
}

func (s *DataServer) SubscribeTopic(req *chat.SubscribeTopicRequest, stream grpc.ServerStreamingServer[chat.MessageEvent]) error {

	log.Printf("SubscribeTopic: user %d subscribing to topics %v", req.GetUserId(), req.GetTopicId())
	// Dummy: keep stream open but don't send anything
	<-stream.Context().Done()
	return nil
}

func (s *DataServer) ReconfigureNode(ctx context.Context, cfg *chat.NodeConfiguration) (*emptypb.Empty, error) {
	// check auth
	err := s.validateSecretKey(ctx)

	if err != nil {
		return nil, err
	}

	s.dataLock.Lock()
	defer s.dataLock.Unlock()

	// TODO: impl

	s.config = cfg
	log.Printf("ReconfigureNode: predecessor=%v, successor=%v, topics=%v",
		cfg.GetPredecessor(), cfg.GetSuccessor(), cfg.GetTopicIds())

	return &emptypb.Empty{}, nil
}

func (s *DataServer) UpdateSubscriptions(ctx context.Context, cfg *chat.NodeCfgSubscriptions) (*emptypb.Empty, error) {
	s.dataLock.Lock()
	defer s.dataLock.Unlock()

	log.Printf("UpdateSubscriptions: topics=%v", cfg.GetTopicIds())
	return &emptypb.Empty{}, nil
}

func (s *DataServer) Ping(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

// ConnectToControl establishes a connection to the control plane server
func (s *DataServer) ConnectToControl() error {
	conn, err := grpc.NewClient(s.Control, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to control server: %w", err)
	}
	s.controlConn = conn
	s.controlCtx = context.Background()
	s.controlCtx = metadata.AppendToOutgoingContext(s.controlCtx, SecretKeyHeader, s.SecretKey)
	s.controlClient = chat.NewControlPlaneClient(conn)
	log.Printf("Connected to control server at %s", s.Control)
	return nil
}

func (s *DataServer) connectTo(address string) (chat.MessageBoardClient, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to node at %s: %w", address, err)
	}
	client := chat.NewMessageBoardClient(conn)
	return client, nil
}

func (s *DataServer) closeConnection(c *grpc.ClientConn) {
	if c != nil {
		_ = c.Close()
	}
}

// CloseControlConnection closes the control plane connection
func (s *DataServer) CloseControlConnection() error {
	if s.controlConn != nil {
		return s.controlConn.Close()
	}
	return nil
}

// authContext creates a context with the secret key in metadata
func (s *DataServer) authContext(ctx context.Context) context.Context {
	md := metadata.Pairs(SecretKeyHeader, s.SecretKey)
	return metadata.NewOutgoingContext(ctx, md)
}

// JoinCluster registers this node with the control plane
func (s *DataServer) JoinCluster(ctx context.Context) (*chat.JoinClusterResponse, error) {
	if s.controlClient == nil {
		return nil, fmt.Errorf("not connected to control server")
	}

	resp, err := s.controlClient.JoinCluster(s.authContext(ctx), &chat.JoinClusterRequest{
		Self: s.Address,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to join cluster: %w", err)
	}

	log.Printf("Joined cluster: you=%d, front=%v, back=%v", resp.GetYou(), resp.GetFront(), resp.GetBack())
	return resp, nil
}

// GetClusterState retrieves the current cluster state from the control plane
func (s *DataServer) GetClusterState(ctx context.Context) (*chat.GetClusterStateResponse, error) {
	if s.controlClient == nil {
		return nil, fmt.Errorf("not connected to control server")
	}

	resp, err := s.controlClient.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster state: %w", err)
	}

	return resp, nil
}

func (s *DataServer) syncFromFront() {
	s.dataLock.Lock()
	defer s.dataLock.Unlock()

	if s.frontClient == nil {
		panic("")
	}

	// TODO: sync with front node
	// TODO: notify control/front about this
}

func StartDataServer(address, control, secretKey string) (*grpc.Server, error) {
	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)
	ds := NewDataServer(address, control, secretKey)
	chat.RegisterMessageBoardServer(grpcServer, ds)

	err := ds.ConnectToControl()
	if err != nil {
		return nil, err
	}

	go (func() {
		joinCtx, cancel := context.WithTimeout(ds.controlCtx, 5*time.Second)
		defer cancel()

		r, err := ds.JoinCluster(joinCtx)

		ds.front = r.GetFront()
		ds.back = r.GetBack()

		if ds.front != nil {
			ds.frontClient, err = ds.connectTo(ds.front.Address)
			if err != nil {
				panic(err)
			}

			ds.syncFromFront()
		} else {
			log.Print("Head!")
		}

		if ds.back != nil {
			panic("Unexpected insertion position")
		}

		if err != nil {
			panic(err)
		}
	})()

	return grpcServer, nil
}
