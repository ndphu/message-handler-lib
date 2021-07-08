# message-handler-lib

## A library to consume Skype Message Event
Skype Message Events are publised to a worker's fanout exchange. This library helps to create a consumer queue to consume messages from that fanout.

## Usage:
To listen for message from a worker, the environment variable "WORKER_ID"should be provided.

### Example code:
```go
package main

import (
	"github.com/ndphu/message-handler-lib/config"
	"github.com/ndphu/message-handler-lib/handler"
	"github.com/ndphu/message-handler-lib/model"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	workerId, consumerId := config.LoadConfig()
	eventHandler, err := handler.NewEventHandler(handler.EventHandlerConfig{
		WorkerId:            workerId,
		ConsumerId:          consumerId,
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
	// just print message details to console
	log.Printf("Received message\nFrom=%s\nTo=%s\nContent=%s\n",
		e.GetFrom(), e.GetThreadId(), e.GetContent())
}
```
