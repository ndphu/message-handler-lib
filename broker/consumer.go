package broker

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

var ErrorConsumerWorkerStopTimeout = errors.New("ConsumerWorkerStopTimeout")

type ConsumerWorker struct {
	consumerId     string
	queueName      string
	ctx            context.Context
	cancel         context.CancelFunc
	stopped        chan bool
	handler        DeliveryHandler
	consumeChannel *amqp.Channel
	running        bool
}

type QueueConsumer struct {
	consumers        []*ConsumerWorker
	ctx              context.Context
	cancel           context.CancelFunc
	stopped          chan bool
	handler          DeliveryHandler
	queueName        string
	consumerCount    int
	connChangedSubId string
}

type DeliveryHandler func(d amqp.Delivery)

func NewQueueConsumer(queueName string, consumerCount int, handler DeliveryHandler) (*QueueConsumer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	var qc = &QueueConsumer{
		cancel:        cancel,
		ctx:           ctx,
		stopped:       make(chan bool, 1),
		handler:       handler,
		queueName:     queueName,
		consumerCount: consumerCount,
	}
	qc.connChangedSubId = rmqConnection.AddConnectionChangedListener(func(e *amqp.Error) {
		qc.Log("Connection closed.")
		_ = qc.doStop()
		if err := qc.Start(); err != nil {
			log.Println("Fail to restart...", err.Error())
		}
	})
	return qc, nil
}

func (qc *QueueConsumer) Start() error {
	worker := make([]*ConsumerWorker, qc.consumerCount)
	for i := 0; i < qc.consumerCount; i++ {
		worker[i] = NewWorker(qc.queueName, qc.handler)
	}
	qc.consumers = worker
	for _, cw := range qc.consumers {
		if err := cw.Start(); err != nil {
			return err
		}
	}
	return nil
}

func (qc *QueueConsumer) Stop() error {
	defer func() {
		rmqConnection.RemoveConnectionChangedListener(qc.connChangedSubId)
	}()
	return qc.doStop()
}

func (qc *QueueConsumer) doStop() error {
	wg := sync.WaitGroup{}

	for _, rc := range qc.consumers {
		wg.Add(1)
		go func(cw *ConsumerWorker) {
			defer wg.Done()
			if err := cw.Stop(); err != nil {
				qc.Log("Fail to stop Consumer Worker", cw.consumerId, err.Error())
			} else {
				qc.Log("Consumer Worker", cw.consumerId, "stopped successfully.")
			}
		}(rc)
	}
	wg.Wait()

	qc.Log("Stopped successfully")

	return nil
}

func (qc *QueueConsumer) Log(args ...interface{}) {
	msg := ""
	for _, arg := range args {
		msg = msg + fmt.Sprintf("%s ", arg)
	}
	log.Println("QueueConsumer", "-", qc.queueName, "-", msg)
}

func NewWorker(queueName string, handler DeliveryHandler) *ConsumerWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConsumerWorker{
		consumerId: "consumer-" + uuid.New().String(),
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
	log.Printf("ConsumerWorker - %s - %s - %s\n", cw.consumerId, tag, msg)
}

func (cw *ConsumerWorker) Start() error {
	channel, msgs, err := Consume(cw.queueName, cw.consumerId)
	if err != nil {
		cw.Log("Start", "Fail to consume message from queue", cw.queueName, "by error", err.Error())
		return err
	}
	cw.consumeChannel = channel
	go func() {
		for {
			select {
			case d, ok := <-msgs:
				{
					if !ok {
						// channel closed or queue deleted
						cw.running = false
						return
					}
					d.Ack(false)
					cw.handler(d)
				}
			case <-cw.ctx.Done():
				cw.doStop()
				cw.running = false
				return
			}
		}
	}()
	cw.Log("Start", "Started successfully")
	cw.running = true
	return nil
}

func (cw *ConsumerWorker) Stop() error {
	timer := time.NewTimer(5 * time.Second)
	cw.cancel()
	select {
	case <-cw.stopped:
		return nil
	case <-timer.C:
		return ErrorConsumerWorkerStopTimeout
	}
}

func (cw *ConsumerWorker) doStop() error {
	cw.Log("Stop", "Stopping...")
	defer func() {
		select {
		case cw.stopped <- true:
			return
		default:
			// stop signal not send
		}
	}()
	if err := StopConsume(cw.consumeChannel, cw.consumerId); err != nil {
		cw.Log("Stop", "Fail to stopConsume by error", err.Error())
	}

	if err := cw.consumeChannel.Close(); err != nil {
		cw.Log("Stop", "Fail to stopConsume by error", err.Error())
	}

	cw.Log("Stop", "Stopped successfully")
	return nil
}

func (cw *ConsumerWorker) QueueName() string {
	return cw.queueName
}

func (qc *QueueConsumer) QueueName() string {
	return qc.queueName
}
