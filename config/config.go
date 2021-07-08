package config

import (
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"os"
)

func LoadConfig() (workerId string, consumerId string) {
	wid := os.Getenv("WORKER_ID")
	if wid == "" {
		log.Fatalf("WORKER_ID is not defined")
	}
	cid := os.Getenv("CONSUMER_ID")
	if cid == "" {
		log.Println("Cannot load consumer ID from environment variable CONSUMER_ID")
		if bytes, err := ioutil.ReadFile(".consumer_id"); err != nil || len(bytes) == 0 {
			cid = uuid.New().String()
			log.Println("Generated consumer ID", cid)
			if err := ioutil.WriteFile(".consumer_id", []byte(cid), 0755); err != nil {
				log.Fatalln("Fail to write consumer id to file", err)
			}
		} else {
			cid = string(bytes)
			log.Println("Loaded consumer ID from saved file", cid)
		}
	}
	return wid, cid
}
