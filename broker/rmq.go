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

func (rc *RmqConnection) NewBroker() (*Broker, error) {
	if channel, err := rc.NewChannel(); err != nil {
		return nil, err
	} else {
		if err := channel.Qos(250,
			0,
			false); err != nil {
			log.Println("Fail to setup Qos for channel", err.Error())
			return nil, err
		}
		b := &Broker{
			connection: rc.conn,
			channel:    channel,
		}
		return b, nil
	}
}

type Broker struct {
	connection    *amqp.Connection
	channel       *amqp.Channel
	closeCallback OnConnectionChanged
}

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

func (rc *RmqConnection) NewChannel() (*amqp.Channel, error) {
	return rc.conn.Channel()
}

func (rc *RmqConnection) AddConnectionChangedListener(closedListener OnConnectionChanged) {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	rc.OnConnectionChangedListeners = append(rc.OnConnectionChangedListeners, closedListener)
}

func (r *Broker) DeclareQueue(queueName string) (amqp.Queue, error) {
	return r.channel.QueueDeclare(
		queueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
}

func (r *Broker) DeclareFanout(exchange string) (error) {
	return r.channel.ExchangeDeclare(
		exchange,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
}

func (r *Broker) BindFanout(queueName string, exchangeName string) error {
	return r.channel.QueueBind(
		queueName,
		"",
		exchangeName,
		false,
		nil)
}

func (r *Broker) PublishToQueue(queueName string, contentType string, body []byte) error {
	return r.channel.Publish(
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

func (r *Broker) PublishTextToQueue(queueName string, message string) error {
	return r.PublishToQueue(queueName, "text/plain", []byte(message))
}

func (r *Broker) PublishToFanoutExchange(exchange string, contentType string, body []byte) error {
	return r.channel.Publish(exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: contentType,
			Body:        body,
		})
}

func (r *Broker) PublishTextToFanoutExchange(exchange string, message string) error {
	return r.PublishToFanoutExchange(exchange, "text/plain", []byte(message))
}

func (r *Broker) Shutdown() {
	log.Println("Shutting down broker...")
	r.channel.Close()
	//r.connection.Close()
}
func (r *Broker) SetupQos() error {
	return r.channel.Qos(1,
		0,
		false)
}

func (r *Broker) Consume(queueName string, consumerTag string) (<-chan amqp.Delivery, error) {
	return r.channel.Consume(
		queueName,
		consumerTag, // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
}

func (r *Broker) Ack(tag uint64, multiple bool) error {
	return r.channel.Ack(tag, multiple)
}

func (r *Broker) StopConsume(consumerTag string) (error) {
	return r.channel.Cancel(consumerTag, false)

}

func (r *Broker) PublishRpcRequest(rpcQueue string, replyTo string, corrId string, request *RpcRequest) error {
	data, err := json.Marshal(request)
	if err != nil {
		log.Println("RPC: Fail to marshall RPC request")
		return err
	}
	return r.channel.Publish("", // exchange
		rpcQueue,                // routing key
		false,                   // mandatory
		false,                   // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       replyTo,
			Body:          data,
		})
}

func (r *Broker) PublishRpcResponse(d amqp.Delivery, response string) error {
	if d.ReplyTo == "" {
		// message has no reply-to queue
		return nil
	}
	return r.channel.Publish(
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
func (r *Broker) RemoveQueue(queueName string) (int, error) {
	return r.channel.QueueDelete(queueName,
		false,
		false,
		false)
}
func (r *Broker) RemoveExchange(exchange string) error {
	return r.channel.ExchangeDelete(exchange,
		false,
		false)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
