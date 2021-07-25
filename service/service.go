package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ndphu/message-handler-lib/broker"
	"github.com/ndphu/message-handler-lib/handler"
	"github.com/streadway/amqp"
	"log"
	"time"
)

type Service struct {
	id                      string
	desc                    Description
	discoveryConsumer       *broker.QueueConsumer
	discoveryFanoutExchange string
	discoveryQueue          string
	actions                 []Action
	rpcQueue                string
	rpcHandler              *handler.RpcHandler
}

func NewService(id string, desc Description, actions []Action) *Service {
	return &Service{
		id:      id,
		desc:    desc,
		actions: actions,
	}
}

type Description struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Version string `json:"version"`
}

type DiscoveryRequest struct {
	ServiceType string `json:"serviceType"`
	ReplyTo     string `json:"replyTo"`
}

type DiscoveryResponse struct {
	ServiceId   string      `json:"serviceId"`
	ServiceType string      `json:"serviceType"`
	Timestamp   time.Time   `json:"timestamp"`
	Description Description `json:"description"`
	Actions     []Action    `json:"actions"`
}

func (s *Service) Start() error {
	if err := s.startServiceDiscovery(); err != nil {
		return err
	}
	if err := s.startServiceRpc(); err != nil {
		return err
	}
	return nil
}

func (s *Service) startServiceDiscovery() error {
	s.discoveryFanoutExchange = "/services/discovery"
	s.discoveryQueue = fmt.Sprintf("/service/%s/discovery", s.id)
	log.Println("Declaring service discovery fanout")
	if err := broker.DeclareFanout(s.discoveryFanoutExchange); err != nil {
		return err
	}
	log.Println("Declaring discovery queue for service")
	if _, err := broker.DeclareQueue(s.discoveryQueue); err != nil {
		return err
	}

	log.Println("Binding service discovery queue", s.discoveryQueue, "to discovery fanout", s.discoveryFanoutExchange)
	if err := broker.BindFanout(s.discoveryQueue, s.discoveryFanoutExchange); err != nil {
		return err
	}

	log.Println("Declaring consumer for discovery queue", s.discoveryQueue)
	discoveryConsumer, err := broker.NewQueueConsumer(s.discoveryQueue, 1, func(d amqp.Delivery) {
		if len(d.Body) == 0 {
			return
		}
		dm := DiscoveryRequest{}
		if err := json.Unmarshal(d.Body, &dm); err != nil {
			log.Println("Fail to unmarshall discovery message by error", err.Error())
			return
		}
		if dm.ReplyTo != "" && (dm.ServiceType == "*" || dm.ServiceType == s.desc.Type) {
			log.Println("Replying discovering message to", dm.ReplyTo)
			dr := DiscoveryResponse{
				ServiceId:   s.id,
				ServiceType: s.desc.Type,
				Timestamp:   time.Now(),
				Description: s.desc,
				Actions:     s.actions,
			}
			if marshal, err := json.Marshal(dr); err != nil {
				log.Println("Fail to marshall discovery response")
			} else {
				if err := broker.PublishTextToQueue(dm.ReplyTo, string(marshal)); err != nil {
					log.Println("Fail to publish discovery response to queue", dm.ReplyTo, "by error", err.Error())
				} else {
					log.Println("Replied to discovery request to queue", dm.ReplyTo)
				}
			}
		} else {
			log.Printf("Ignore discovering message %s\n", string(d.Body))
		}
	})
	if err != nil {
		return err
	}
	log.Println("Starting consumer for discovery queue", s.discoveryQueue)
	if err := discoveryConsumer.Start(); err != nil {
		return err
	}
	s.discoveryConsumer = discoveryConsumer
	return nil
}

func (s *Service) Stop() error {
	defer func() {
		broker.RemoveQueue(s.discoveryQueue)
	}()

	if s.discoveryConsumer != nil {
		return s.discoveryConsumer.Stop()
	}
	return nil
}

func (s *Service) startServiceRpc() error {
	if rpcHandler, err := handler.NewRpcHandler(s.id, func(request broker.RpcRequest) (interface{}, error) {
		return s.handleRpc(request)
	}); err != nil {
		return err
	} else {
		s.rpcHandler = rpcHandler
	}
	s.rpcHandler.Start()
	return nil
}

func (s *Service) handleRpc(req broker.RpcRequest) (interface{}, error) {
	for _, action := range s.actions {
		if action.Name == req.Method {
			if err := action.ValidateArgs(req.Args); err != nil {
				return nil, err
			}
			return action.Handler(req.Args)
		}
	}
	return nil, errors.New("ActionNotFound")
}
