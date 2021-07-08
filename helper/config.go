package helper

import (
	"github.com/google/uuid"
	"github.com/ndphu/message-handler-lib/broker"
	"io/ioutil"
	"log"
	"os"
)

func LoadConfig() (string, string) {
	workerId := os.Getenv("WORKER_ID")
	if workerId == "" {
		log.Fatalf("WORKER_ID is not defined")
	}
	consumerId := os.Getenv("CONSUMER_ID")
	if consumerId == "" {
		log.Println("Cannot load consumer ID from environment variable CONSUMER_ID")
		if bytes, err := ioutil.ReadFile(".consumer_id"); err != nil || len(bytes) == 0 {
			consumerId = uuid.New().String()
			log.Println("Generated consumer ID", consumerId)
			if err := ioutil.WriteFile(".consumer_id", []byte(consumerId), 0755); err != nil {
				log.Fatalln("Fail to write consumer id to file", err)
			}
		} else {
			consumerId = string(bytes)
			log.Println("Loaded consumer ID from saved file", consumerId)
		}
	}
	exchange := "/worker/" + workerId + "/textMessages"
	queueName := "/message-handler/example-message-handler-" + consumerId
	return exchange, queueName
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
