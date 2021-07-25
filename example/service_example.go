package main

import (
	"github.com/google/uuid"
	"github.com/ndphu/message-handler-lib/service"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	serviceId := os.Getenv("SERVICE_ID")
	if serviceId == "" {
		serviceId = uuid.New().String()
	}
	s := service.NewService(serviceId,
		service.Description{
			Name:    "example-service",
			Type:    "example",
			Version: "0.0.1-demo",
		},
		[]service.Action{
			{
				Name:          "example:echo",
				ArgumentCount: 1,
				Handler: func(args []string) (interface{}, error) {
					return service.ActionResult{
						"echo": args[0],
					}, nil
				},
			},
			{
				Name:          "example:sum",
				ArgumentCount: 2,
				Handler: func(args []string) (interface{}, error) {
					a, err := strconv.Atoi(args[0])
					if err != nil {
						return "", err
					}
					b, err := strconv.Atoi(args[1])
					if err != nil {
						return "", err
					}
					return service.ActionResult{
						"result": a + b,
					}, nil
				},
			},
		})
	if err := s.Start(); err != nil {
		panic(err)
	}

	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	<-termChan
	log.Println("Shutdown signal received")
	_ = s.Stop()

}
