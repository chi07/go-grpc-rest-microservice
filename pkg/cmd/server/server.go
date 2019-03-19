package cmd

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/shinichi2510/go-grpc-rest-microservice/pkg/protocol/grpc"
	"github.com/shinichi2510/go-grpc-rest-microservice/pkg/service/v1"
)

type Config struct {
	GRPCPort            string
	DatastoreDBHost     string
	DatastoreDBUser     string
	DatastoreDBPassword string
	DatastoreDBSchema   string
}

// RunServer runs gRPC server and HTTP Gateway
func RunServer() error {
	ctx := context.Background()

	// get configuration
	var cfg Config

	flag.StringVar(&cfg.GRPCPort, "grpc-port", "", "gRPC port to bind")
	flag.StringVar(&cfg.DatastoreDBHost, "db-host", "", "DB host")
	flag.StringVar(&cfg.DatastoreDBUser, "db-user", "", "DB user")
	flag.StringVar(&cfg.DatastoreDBPassword, "db-password", "", "DB password")
	flag.StringVar(&cfg.DatastoreDBSchema, "db-schema", "", "DB Schema")

	fmt.Println(cfg)

	if len(cfg.GRPCPort) == 0 {
		return fmt.Errorf("invalid port for gRPC server: '%s'", cfg.GRPCPort)
	}

	// Add mysql driver
	param := "parseTime=true"

	dns := fmt.Sprintf("%s:%s@tcp(%s)/%s?%s",
		cfg.DatastoreDBUser,
		cfg.DatastoreDBPassword,
		cfg.DatastoreDBHost,
		cfg.DatastoreDBSchema,
		param)

	db, err := sql.Open("mysql", dns)
	if err != nil {
		return fmt.Errorf("could not connect to database %v", err)
	}
	defer db.Close()

	v1API := v1.NewTodoServiceServer(db)

	return grpc.RunServer(ctx, v1API, cfg.GRPCPort)
}
