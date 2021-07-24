package broker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"time"
)

var ErrorInvalidRpcClientConfig = errors.New("InvalidRpcClientConfig")

type RpcRequest struct {
	Method string   `json:"method"`
	Args   []string `json:"args"`
}

type RpcResponse struct {
	Request  RpcRequest `json:"request"`
	Response string     `json:"response"`
	Success  bool       `json:"success"`
	Error    string     `json:"error"`
}

const DefaultRpcExchange = "/defaultRpcExchange"

type RpcClient struct {
	timeout     time.Duration
	unitId      string
	rpcExchange string
}

type ExecResult struct {
	response *RpcResponse
	err      error
}

type RpcConfig struct {
	UnitId      string        `json:"unitId"`
	Timeout     time.Duration `json:"timeout"`
	RpcExchange string        `json:"rpcExchange"`
}

func NewRpcClient(unitId string) (*RpcClient, error) {
	return NewRpcClientWithConfig(RpcConfig{
		UnitId:      unitId,
		Timeout:     30 * time.Second,
		RpcExchange: DefaultRpcExchange,
	})
}

func NewRpcClientWithConfig(c RpcConfig) (*RpcClient, error) {
	if c.UnitId == "" || c.RpcExchange == "" {
		return nil, ErrorInvalidRpcClientConfig
	}
	return &RpcClient{
		unitId:  c.UnitId,
		timeout: c.Timeout,
		rpcExchange: c.RpcExchange,
	}, nil
}

func (c *RpcClient) Send(request *RpcRequest) error {
	channel, err := NewChannel()
	if err != nil {
		log.Printf("RPC: Fail to create channel to send RPC request. Error = %v\n", err)
		return err
	}
	defer channel.Close()
	log.Println("RPC: Sending RPC request to RPC exchange", c.rpcExchange, "with routingKey", c.unitId)
	data, err := json.Marshal(request)
	if err != nil {
		log.Println("RPC: Fail to marshall RPC request")
		return err
	}
	return channel.Publish(c.rpcExchange, // exchange
		c.unitId, // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		})
}

func (c *RpcClient) SendAndReceive(request *RpcRequest) (*RpcResponse, error) {
	channel, err := NewChannel()
	if err != nil {
		log.Printf("RCP: Fail to create channel to send RPC request. Error = %v\n", err)
		return nil, err
	}
	defer channel.Close()
	if err != nil {
		log.Println("Fail to create broker", err.Error())
	}

	replyTo, err := channel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)

	if err != nil {
		log.Println("RPC: Fail to declare reply to queue")
		return nil, err
	}

	log.Println("RPC: Declared replyTo queue", replyTo.Name)

	replyChannel, msgs, err := Consume(replyTo.Name, "")
	defer replyChannel.Close()

	if err != nil {
		log.Println("RPC: Fail to consume replyTo queue")
		return nil, err
	}

	corrId := randomString(32)

	log.Println("RPC: Sending RPC request to RPC exchange", c.rpcExchange, "with routing key", c.unitId)

	data, err := json.Marshal(request)
	if err != nil {
		log.Println("RPC: Fail to marshall RPC request")
		return nil, err
	}
	if err := channel.Publish(c.rpcExchange, // exchange
		c.unitId, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       replyTo.Name,
			Body:          data,
		}); err != nil {
		return nil, err
	}

	cx, cancel := context.WithCancel(context.Background())
	timer := time.NewTimer(c.timeout)

	go func() {
		<-timer.C
		log.Println("RPC: Timeout detected.")
		cancel()
	}()

	delivery := make(chan amqp.Delivery)

	execResult := make(chan ExecResult)

	go func() {
		select {
		case d := <-delivery:
			{
				var response RpcResponse
				err := json.Unmarshal(d.Body, &response)
				execResult <- ExecResult{
					response: &response,
					err:      err,
				}
				timer.Stop()
				break
			}
		case <-cx.Done():
			{
				execResult <- ExecResult{
					response: nil,
					err:      errors.New("NoReplyAfterTimeout"),
				}
			}
		}
	}()
	go func() {
		for d := range msgs {
			if corrId == d.CorrelationId {
				delivery <- d
				break
			}
		}
	}()

	result := <-execResult

	return result.response, result.err
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}
