package broker

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"log"
	"sync"
)

type ConsumerWorker struct {
	consumerId string
	broker     *Broker
	queueName  string
	ctx        context.Context
	cancel     context.CancelFunc
	stopped    chan bool
	handler    DeliveryHandler
}

type QueueConsumer struct {
	consumers []*ConsumerWorker
	ctx       context.Context
	cancel    context.CancelFunc
	stopped   chan bool
	handler   DeliveryHandler
}

type DeliveryHandler func(d amqp.Delivery)

func NewQueueConsumer(queueName string, consumerCount int, handler DeliveryHandler) (*QueueConsumer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	worker := make([]*ConsumerWorker, consumerCount)
	var cm = &QueueConsumer{
		cancel:  cancel,
		ctx:     ctx,
		stopped: make(chan bool, 1),
		handler: handler,
	}

	for i := 0; i < consumerCount; i++ {
		newBroker, err := NewBroker()
		if err != nil {
			cm.Log("Fail to init worker", i)
			return nil, err
		}
		worker[i] = NewWorker(newBroker, queueName, handler)
	}
	cm.consumers = worker

	return cm, nil
}

func (qc *QueueConsumer) Start() {
	for _, cw := range qc.consumers {
		cw.Start()
	}
}

func (qc *QueueConsumer) Stop() error {
	wg := sync.WaitGroup{}

	for _, rc := range qc.consumers {
		wg.Add(1)
		go func(_rc *ConsumerWorker) {
			defer wg.Done()
			if err := _rc.Stop(); err != nil {
				qc.Log("Fail to stop Consumer Worker", _rc.consumerId)
			} else {
				qc.Log("Consumer Worker", _rc.consumerId, "stopped successfully.")
			}
		}(rc)
	}
	wg.Wait()

	return nil
}

func (qc *QueueConsumer) Log(args ... interface{}) {
	log.Println("QueueConsumer:", args)
}

func NewWorker(broker *Broker, queueName string, handler DeliveryHandler) *ConsumerWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConsumerWorker{
		consumerId: "consumer-" + uuid.New().String(),
		broker:     broker,
		queueName:  queueName,
		ctx:        ctx,
		cancel:     cancel,
		stopped:    make(chan bool, 1),
		handler:    handler,
	}
}

func (cw *ConsumerWorker) Log(tag string, args ...interface{}) {
	msg := ""
	for _, arg := range args {
		msg = msg + fmt.Sprintf("%s ", arg)
	}
	log.Printf("Worker - %s - %s - %s\n", cw.consumerId, tag, args)
}

func (cw *ConsumerWorker) Start() error {
	msgs, err := cw.broker.Consume(cw.queueName, cw.consumerId)
	if err != nil {
		cw.Log("Start", "Fail to consume message from RPC queue", cw.queueName, "by error", err.Error())
		return err
	}

	go func() {
		for {
			select {
			case d := <-msgs:
				{
					d.Ack(false)
					cw.handler(d)
				}
			case <-cw.ctx.Done():
				cw.doStop()
				return
			}
		}
	}()
	cw.Log("Start", "Started successfully")

	return nil
}

func (cw *ConsumerWorker) Stop() error {
	cw.cancel()
	<-cw.stopped
	return nil
}

func (cw *ConsumerWorker) doStop() error {
	cw.Log("Stop", "Stopping...")
	defer func() { cw.stopped <- true }()
	if err := cw.broker.StopConsume(cw.consumerId); err != nil {
		cw.Log("Stop", "Fail to stopConsume by error", err.Error())
		return err
	}
	cw.Log("Stop", "Stopped successfully")
	return nil
}
