package handler

import (
	"encoding/json"
	"github.com/ndphu/message-handler-lib/broker"
	"github.com/streadway/amqp"
	"log"
)

type RpcHandler struct {
	queueName   string
	consumer    *broker.QueueConsumer
	removeQueue bool
}

type RpcHandlerConfig struct {
	WorkerId      string
	RemoveQueue   bool
	ConsumerCount int
}

type OnNewRpcRequest func(request *broker.RpcRequest)

func NewRpcHandler(c RpcHandlerConfig, callback OnNewRpcRequest) (*RpcHandler, error) {
	queueName := "/worker/" + c.WorkerId + "/rpc_queue"

	if _, err := broker.DeclareQueue(queueName); err != nil {
		log.Println("RpcHandler - Fail to declare queue", queueName)
		return nil, err
	}

	qc, err := broker.NewQueueConsumer(queueName, c.ConsumerCount, func(d amqp.Delivery) {
		if len(d.Body) > 0 {
			var request *broker.RpcRequest
			if err := json.Unmarshal(d.Body, &request); err != nil {
				log.Println("RpcHandler - Fail to unmarshal delivery body by error", err.Error())
			} else {
				if callback != nil {
					callback(request)
				}
			}
		} else {
			log.Println("RpcHandler - Fail to process delivery with empty body.")
		}
	})
	if err != nil {
		log.Println("RpcHandler - Fail to create consumer for queue", queueName)
		return nil, err
	}

	return &RpcHandler{
		queueName:   queueName,
		consumer:    qc,
		removeQueue: c.RemoveQueue,
	}, nil
}

func (rh *RpcHandler) Start() error {
	rh.consumer.Start()
	return nil
}

func (rh *RpcHandler) Stop() error {
	defer func() {
		if rh.removeQueue {
			log.Println("RpcHandler - Removing queue", rh.queueName)
			if _, err := broker.RemoveQueue(rh.queueName); err != nil {
				log.Println("RpcHandler - Fail to remove queue", rh.queueName)
			}
		}
	}()
	return rh.consumer.Stop()
}

func (rh *RpcHandler) QueueName() string {
	return rh.queueName
}
