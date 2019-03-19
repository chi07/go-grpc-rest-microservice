package main

import (
	"fmt"
	"github.com/shinichi2510/go-grpc-rest-microservice/pkg/cmd/server"
	"os"
)

func main()  {
	if err := cmd.RunServer(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
