package main

import (
	"context"
	"fmt"
	"log"

	chat "github.com/Serial-Experiments-Weebify/weebchat/grpc"
	"github.com/wailsapp/wails/v2/pkg/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type App struct {
	ctx       context.Context
	head      chat.MessageBoardClient
	tail      chat.MessageBoardClient
	cancelSub context.CancelFunc
	subCtx    context.Context
}

func NewApp() *App {
	return &App{}
}

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
}

// Connect is called from the UI when the user enters the server address
func (a *App) Connect(addr string) string {

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to control plane: %v", err)
	}
	ctrl := chat.NewControlPlaneClient(conn)

	// 2. Get state using emptypb.Empty
	state, err := ctrl.GetClusterState(context.Background(), &emptypb.Empty{})
	if err != nil {
		log.Fatalf("Could not get cluster state: %v", err)
	}

	if state.Head == nil || state.Tail == nil {
		log.Fatal("Cluster is not ready (no data nodes joined yet).")
	}

	// 3. Connect to Head (Writes) and Tail (Reads)
	hConn, _ := grpc.NewClient(state.Head.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	tConn, _ := grpc.NewClient(state.Tail.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))

	a.head = chat.NewMessageBoardClient(hConn)
	a.tail = chat.NewMessageBoardClient(tConn)

	return "Connected to " + addr
}

func (a *App) PostMessage(topicID, userID int64, text string) string {
	_, err := a.head.PostMessage(context.Background(), &chat.PostMessageRequest{
		TopicId: topicID,
		UserId:  userID,
		Text:    text,
	})
	if err != nil {
		return err.Error()
	}
	return "Success"
}

func (a *App) StartSubscription(userID, topicID int64) {
	if a.cancelSub != nil {
		a.cancelSub()
	}

	// Create a new context for this subscription
	a.subCtx, a.cancelSub = context.WithCancel(a.ctx)

	subNode, err := a.head.GetSubscriptionNode(a.subCtx, &chat.SubscriptionNodeRequest{
		UserId:  userID,
		TopicId: topicID,
	})
	if err != nil {
		log.Fatalf("Load balancer error: %v", err)
	}

	// Connect to the node chosen by the server
	conn, err := grpc.NewClient(subNode.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to node: %v", err)
	}

	subClient := chat.NewMessageBoardClient(conn)

	go func() {
		stream, err := subClient.SubscribeTopic(a.subCtx, &chat.SubscribeTopicRequest{
			UserId:  userID,
			TopicId: topicID,
		})
		if err != nil {
			return
		}

		for {
			event, err := stream.Recv()
			if err != nil {
				break
			}
			runtime.EventsEmit(a.ctx, "message_event", event.Message)
		}
	}()
}

func (a *App) CreateUser(name string) (*chat.User, error) {
	user, err := a.head.CreateUser(context.Background(), &chat.CreateUserRequest{
		Name: name,
	})
	if err != nil {
		return nil, err
	}

	// Optional: Log it in the Go terminal to verify
	log.Printf("User created via UI: %s (ID: %d)", user.Name, user.Id)
	return user, nil
}

func (a *App) FetchTopics() ([]*chat.Topic, error) {
	// Reads should go to the Tail node
	resp, err := a.tail.ListTopics(context.Background(), &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	log.Printf("Num of topics: %d", len(resp.Topics))
	return resp.Topics, nil
}

func (a *App) CreateTopic(name string) (*chat.Topic, error) {
	log.Print("TEST")
	topic, err := a.head.CreateTopic(context.Background(), &chat.CreateTopicRequest{
		Name: name,
	})
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	log.Printf("Topic created: %s (ID: %d)\n", topic.Name, topic.Id)

	return topic, nil
}

func (a *App) LikeMessage(tID, mID, uID int64) {
	res, err := a.head.LikeMessage(context.Background(), &chat.LikeMessageRequest{
		TopicId:   tID,
		MessageId: mID,
		UserId:    uID,
	})
	if err != nil {
		log.Fatalf("Error liking message: %v", err)
	}

	fmt.Printf("Message %d liked! New like count: %d\n", res.Id, res.Likes)
}

func (a *App) DummyMessageBullshit() *chat.Message {
	return &chat.Message{} // stupid codegen bullshit
}
