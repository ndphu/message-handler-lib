package broker

import (
	"encoding/json"
	"errors"
	"github.com/streadway/amqp"
	"log"
	"os"
)

var connection *amqp.Connection = nil

type Broker struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

func NewBroker() (*Broker, error) {
	if channel, err := connection.Channel(); err != nil {
		return nil, err
	} else {
		if err := channel.Qos(250,
			0,
			false); err != nil {
			log.Println("Fail to setup Qos for channel", err.Error())
			return nil, err
		}
		return &Broker{
			connection: connection,
			channel:    channel,
		}, nil
	}
}

func init() {
	var rmqUrl string
	if rmqUrl = os.Getenv("BROKER_URL"); rmqUrl == "" {
		panic(errors.New("BROKER_URL environment variable should be defined"))
	}
	log.Println("Connecting to RMQ", rmqUrl)
	conn, err := amqp.Dial(rmqUrl)
	failOnError(err, "Failed to connect to RabbitMQ")
	channel, err := conn.Channel()
	failOnError(err, "Failed to get channel from connection")
	defer channel.Close()
	connection = conn
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
