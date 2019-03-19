package grpc

import (
	"context"
	"github.com/shinichi2510/go-grpc-rest-microservice/pkg/api/v1"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"os/signal"
)

func RunServer(ctx context.Context, v1API v1.ToDoServiceServer, port string) error {
	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}

	// register service
	server := grpc.NewServer()
	v1.RegisterToDoServiceServer(server, v1API)

	// graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for range c {
			logrus.Println("shutting down gRPC server ...")
			server.GracefulStop()
			<-ctx.Done()
		}
	}()

	// starting server
	log.Println("starting gRPC server")
	return server.Serve(listen)
}
