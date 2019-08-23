package main

import (
	mgo_pubsub "github.com/mobilusoss/mgo-pubsub"
	"log"
	"sync"
	"time"
)

const (
	eventName = "sample_event"
)

func main() {
	pubsub1 := mustPubsub()
	go publishLoop(pubsub1)

	go func() {
		ch := pubsub1.Subscribe(eventName)
		for {
			message := <- *ch
			log.Println("instance:", 1, "message:", message)
		}
	}()

	go func() {
		pubsub2 := mustPubsub()
		ch := pubsub2.Subscribe(eventName)
		for {
			message := <- *ch
			log.Println("instance:", 2, "message:", message)
		}
	}()

	go func() {
		pubsub3 := mustPubsub()
		ch := pubsub3.Subscribe(eventName)
		for {
			message := <- *ch
			log.Println("instance:", 3, "message:", message)
		}
	}()


	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}

func mustPubsub() *mgo_pubsub.PubSub {
	pubsub, err := mgo_pubsub.NewPubSub("mongodb://127.0.0.1:27017", "pubsub_sample", "pubsub")
	if err != nil {
		log.Fatalln(err)
	}
	if err := pubsub.Initialize(); err != nil {
		log.Fatalln(err)
	}
	errCh := pubsub.StartPubSub()
	go func() {
		e := <- errCh
		log.Fatalln(e)
	}()
	return pubsub
}

func publishLoop(pubsub *mgo_pubsub.PubSub) {
	for range time.Tick(time.Second) {
		if err := pubsub.Publish(eventName, "sample_value"); err != nil {
			break
		}
	}
}