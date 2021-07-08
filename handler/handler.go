package handler

import (
	"encoding/json"
	"github.com/ndphu/message-handler-lib/broker"
	"github.com/ndphu/message-handler-lib/model"
	"github.com/ndphu/message-handler-lib/utils"
	"github.com/streadway/amqp"
	"log"
)

type OnNewEvent func(e model.MessageEvent)

type EventHandler struct {
	consumer     *broker.QueueConsumer
	queueName    string
	exchangeName string
}

type EventHandlerConfig struct {
	WorkerId            string
	ConsumerId          string
	ConsumerWorkerCount int
}

func (h *EventHandler) Start() error {
	h.consumer.Start()
	return nil
}

func (h *EventHandler) Stop() error {
	return h.consumer.Stop()
}

func NewEventHandler(c EventHandlerConfig, callback OnNewEvent) (*EventHandler, error) {
	// loading configuration
	exchange, queueName := utils.GetQueueAndExchangeName(c.WorkerId, c.ConsumerId)

	// binding
	if err := SetupMessageQueue(exchange, queueName); err != nil {
		log.Printf("Fail to setup message queue for exchange=%s queue=%s\n", exchange, queueName)
		return nil, err
	}

	consumer, err := broker.NewQueueConsumer(queueName, c.ConsumerWorkerCount, func(d amqp.Delivery) {
		if callback != nil {
			var e model.MessageEvent
			if len(d.Body) > 0 {
				if err := json.Unmarshal(d.Body, &e); err != nil {
					log.Println("Fail to parse delivery payload", err.Error())
				} else {
					callback(e)
				}
			}
		}
	})
	if err != nil {
		log.Printf("Fail to create consumer for queue=%s error=%v\n", queueName, err)
		return nil, err
	}

	return &EventHandler{
		consumer:     consumer,
		queueName:    queueName,
		exchangeName: exchange,
	}, nil
}

func SetupMessageQueue(exchangeName, queueName string) error {
	b, err := broker.NewBroker()
	if err != nil {

	}
	queue, err := b.DeclareQueue(queueName)
	if err != nil {
		log.Fatalf("Fail to declare queue %s by error %v\n", queueName, err)
	}
	log.Println("Declared queue", queue.Name)
	if err := b.BindFanout(queueName, exchangeName); err != nil {
		log.Fatalf("Fail to bind queue %s to fanout %s by error %v\n", queueName, exchangeName, err)
	}
	return err
}
