package main

import (
	"github.com/ndphu/message-handler-lib/handler"
	"github.com/ndphu/message-handler-lib/model"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	eventHandler, err := handler.NewEventHandler(handler.EventHandlerConfig{
		WorkerId:            "sample-worker",
		ConsumerId:          "sample-consumer-id",
		ConsumerWorkerCount: 8,
		ServiceName:         "example",

	}, func(e model.MessageEvent) {
		processMessageEvent(e)
	})

	if err != nil {
		log.Fatalf("Fail to create handler by error %v\n", err)
	}

	eventHandler.Start()

	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	<-termChan
	log.Println("Shutdown signal received")
	eventHandler.Stop()
}

func processMessageEvent(e model.MessageEvent) {
	log.Printf("Received message\nFrom=%s\nTo=%s\nContent=%s\n",
		e.GetFrom(), e.GetThreadId(), e.GetContent())
}