package broker

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
	"os"
	"sync"
	"time"
)

var rmqConnection *RmqConnection
var connLock = sync.Mutex{}
var sigChan = make(chan *amqp.Error)

type RmqConnection struct {
	conn                         *amqp.Connection
	lock                         sync.Mutex
	OnConnectionChangedListeners []OnConnectionChanged
}

func GetConnection() *RmqConnection {
	return rmqConnection
}

//
//func (rc *RmqConnection) NewBroker() (*Broker, error) {
//	if channel, err := rc.NewChannel(); err != nil {
//		return nil, err
//	} else {
//		if err := channel.Qos(250,
//			0,
//			false); err != nil {
//			log.Println("Fail to setup Qos for channel", err.Error())
//			return nil, err
//		}
//		b := &Broker{
//			connection: rc.conn,
//			channel:    channel,
//		}
//		return b, nil
//	}
//}
//
//type Broker struct {
//	connection    *amqp.Connection
//	channel       *amqp.Channel
//	closeCallback OnConnectionChanged
//}

type OnConnectionChanged func(e *amqp.Error)

func init() {
	reconnectWait := 5
	rmqConnection = &RmqConnection{}
	if err := connect(); err != nil {
		log.Fatalf("Fail to connect to RabbitMQ by error=%v\n", err)
	}

	go func() {
		for {
			e := <-sigChan
			log.Printf("Connection closed. Error = %v\n", e)
			for {
				if err := connect(); err != nil {
					log.Printf("Fail to reconnect by error=%v\n.", err)
					log.Printf("Retry in %d seconds\n", reconnectWait)
					time.Sleep(time.Duration(reconnectWait) * time.Second)
				} else {
					log.Println("Reconnected!")
					rmqConnection.lock.Lock()
					for _, listener := range rmqConnection.OnConnectionChangedListeners {
						go listener(e)
					}
					rmqConnection.lock.Unlock()
					break
				}
			}
		}
	}()
}

func connect() error {
	connLock.Lock()
	conn, err := amqp.Dial(os.Getenv("BROKER_URL"))
	if err != nil {
		return err
	}
	closeChan := make(chan *amqp.Error)
	go func() {
		e := <-closeChan
		sigChan <- e
	}()
	conn.NotifyClose(closeChan)
	rmqConnection.conn = conn
	connLock.Unlock()
	return nil
}

func NewChannel() (*amqp.Channel, error) {
	channel, err := rmqConnection.conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := channel.Qos(250,
		0,
		false); err != nil {
		log.Println("Fail to setup Qos for channel", err.Error())
		return nil, err
	}
	return channel, nil
}

func (rc *RmqConnection) AddConnectionChangedListener(closedListener OnConnectionChanged) {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	rc.OnConnectionChangedListeners = append(rc.OnConnectionChangedListeners, closedListener)
}

func DeclareQueue(queueName string) (amqp.Queue, error) {
	channel, err := NewChannel()
	if err != nil {
		return amqp.Queue{}, err
	}
	defer channel.Close()
	return channel.QueueDeclare(
		queueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
}

func DeclareFanout(exchange string) (error) {
	channel, err := NewChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	return channel.ExchangeDeclare(
		exchange,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
}

func BindFanout(queueName string, exchangeName string) error {
	channel, err := NewChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	return channel.QueueBind(
		queueName,
		"",
		exchangeName,
		false,
		nil)
}

func PublishToQueue(queueName string, contentType string, body []byte) error {
	channel, err := NewChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	return channel.Publish(
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: contentType,
			Body:        body,
		},
	)
}

func PublishTextToQueue(queueName string, message string) error {
	return PublishToQueue(queueName, "text/plain", []byte(message))
}

func PublishToFanoutExchange(exchange string, contentType string, body []byte) error {
	channel, err := NewChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	return channel.Publish(exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: contentType,
			Body:        body,
		})
}

func PublishTextToFanoutExchange(exchange string, message string) error {
	return PublishToFanoutExchange(exchange, "text/plain", []byte(message))
}

func Consume(queueName string, consumerTag string) (*amqp.Channel, <-chan amqp.Delivery, error) {
	channel, err := NewChannel()
	if err != nil {
		return nil, nil, err
	}
	deliveries, err := channel.Consume(
		queueName,
		consumerTag, // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	return channel, deliveries, err
}

func StopConsume(channel *amqp.Channel, comsumerTag string) (error) {
	return channel.Cancel(comsumerTag, false)
}

func PublishRpcRequest(rpcQueue string, replyTo string, corrId string, request *RpcRequest) error {
	data, err := json.Marshal(request)
	if err != nil {
		log.Println("RPC: Fail to marshall RPC request")
		return err
	}
	channel, err := NewChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	return channel.Publish("", // exchange
		rpcQueue,              // routing key
		false,                 // mandatory
		false,                 // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       replyTo,
			Body:          data,
		})
}

func PublishRpcResponse(d amqp.Delivery, response string) error {
	if d.ReplyTo == "" {
		// message has no reply-to queue
		return nil
	}
	channel, err := NewChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	return channel.Publish(
		"",        // exchange
		d.ReplyTo, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: d.CorrelationId,
			Body:          []byte(response),
		})
}
func RemoveQueue(queueName string) (int, error) {
	channel, err := NewChannel()
	if err != nil {
		return -1, err
	}
	defer channel.Close()
	return channel.QueueDelete(queueName,
		false,
		false,
		false)
}
func RemoveExchange(exchange string) error {
	channel, err := NewChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	return channel.ExchangeDelete(exchange,
		false,
		false)
}
