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

type RpcRequestHandler func(request broker.RpcRequest) (interface{}, error)

func NewRpcHandler(c RpcHandlerConfig, handler RpcRequestHandler) (*RpcHandler, error) {
	queueName := "/worker/" + c.WorkerId + "/rpc_queue"

	if _, err := broker.DeclareQueue(queueName); err != nil {
		log.Println("RpcHandler - Fail to declare queue", queueName)
		return nil, err
	}

	qc, err := broker.NewQueueConsumer(queueName, c.ConsumerCount, func(d amqp.Delivery) {
		if len(d.Body) > 0 {
			var request broker.RpcRequest
			if err := json.Unmarshal(d.Body, &request); err != nil {
				log.Println("RpcHandler - Fail to unmarshal delivery body by error", err.Error())
			} else {
				if handler != nil {
					go invokeHandler(d, request, handler)
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
func invokeHandler(d amqp.Delivery, request broker.RpcRequest, handler RpcRequestHandler) {
	result, err := handler(request)

	if d.ReplyTo == "" {
		log.Println("RpcHandler - RPC request does not have replyTo queue")
		return
	}

	resp := broker.RpcResponse{
		Request: request,
		Success: err == nil,
	}

	if err != nil {
		resp.Error = err.Error()
	}

	if result == nil {
		resp.Response = ""
	} else {
		if payload, err := json.Marshal(result); err != nil {
			resp.Error = err.Error()
		} else {
			resp.Response = string(payload)
		}
	}

	if payload, err := json.Marshal(resp); err != nil {
		log.Println("RpcHandler - Fail to marshall RPC response", err.Error())
		return
	} else {
		if err := broker.PublishRpcResponse(d, string(payload)); err != nil {
			log.Println("RpcHandler - Fail to send RPC response to ReplyTo queue", d.ReplyTo)
			return
		}
	}
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
	if rh.consumer != nil {
		return rh.consumer.Stop()
	}
	return nil
}

func (rh *RpcHandler) QueueName() string {
	return rh.queueName
}
