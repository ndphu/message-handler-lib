package handler

import (
	"encoding/json"
	"github.com/ndphu/message-handler-lib/broker"
	"github.com/ndphu/message-handler-lib/model"
	"github.com/streadway/amqp"
	"log"
)

type OnNewEvent func(e model.MessageEvent)

type EventHandler struct {
	consumer     *broker.QueueConsumer
	queueName    string
	exchangeName string
	removeQueue  bool
}

type EventHandlerConfig struct {
	WorkerId            string
	ConsumerId          string
	ConsumerWorkerCount int
	ServiceName         string
	RemoveQueue         bool
}

func (conf EventHandlerConfig) GetQueueAndExchangeName() (string, string) {
	exchange := "/worker/" + conf.WorkerId + "/textMessages"
	queueName := "/message-handler/" + conf.ServiceName + "/queue-" + conf.ConsumerId
	return exchange, queueName
}


func (h *EventHandler) Start() error {
	h.consumer.Start()
	return nil
}

func (h *EventHandler) Stop() error {
	defer func() {
		if h.removeQueue {
			log.Println("EventHandler - Removing queue", h.queueName)
			if _, err := broker.RemoveQueue(h.queueName); err != nil {
				log.Println("EventHandler - Fail to remove queue", h.queueName)
			}
		}
	}()
	return h.consumer.Stop()
}

func (h *EventHandler) QueueName() string {
	return h.queueName
}

func (h *EventHandler) ExchangeName() string {
	return h.exchangeName
}

func NewEventHandler(c EventHandlerConfig, callback OnNewEvent) (*EventHandler, error) {
	// loading configuration
	exchange, queueName := c.GetQueueAndExchangeName()

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
		removeQueue:  c.RemoveQueue,
	}, nil
}

func SetupMessageQueue(exchangeName, queueName string) error {
	queue, err := broker.DeclareQueue(queueName)
	if err != nil {
		log.Printf("EventHandler - Fail to declare queue %s by error %v\n", queueName, err)
		return err
	}
	log.Println("EventHandler - Declared queue", queue.Name)
	if err := broker.BindFanout(queueName, exchangeName); err != nil {
		log.Printf("EventHandler - Fail to bind queue %s to fanout %s by error %v\n", queueName, exchangeName, err)
		return err
	}
	return nil
}
