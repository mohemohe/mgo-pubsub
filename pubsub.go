package mgo_pubsub

import (
	"crypto/tls"
	"errors"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"net"
	"strings"
	"time"
)

type (
	PubSub struct {
		address string
		database string
		collection string
		events map[string][]*chan string
		sess *mgo.Session
	}
	Message struct {
		Event     string    `json:"event" bson:"event"`
		Body      string    `json:"body" bson:"body"`
		Timestamp time.Time `json:"timestamp" bson:"timestamp"`
	}
)

func session(address string) (*mgo.Session, error) {
	dialInfo, err := mgo.ParseURL(address)
	if err != nil {
		return nil, err
	}
	if strings.Contains(strings.ToLower(address), "ssl=true") {
		tlsConfig := &tls.Config{}
		dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
			return conn, err
		}
	}
	return mgo.DialWithInfo(dialInfo)
}

func NewPubSub(address, database, collection string) (*PubSub, error) {
	sess, err := session(address)
	if err != nil {
		return nil, err
	}

	return &PubSub{
		address,
		database,
		collection,
		map[string][]*chan string{},
		sess,
	}, nil
}

func (this *PubSub) Initialize() error {
	this.sess.SetMode(mgo.Strong, true)
	defer this.sess.SetMode(mgo.SecondaryPreferred, true)
	err := this.sess.DB(this.database).C(this.collection).Create(&mgo.CollectionInfo{
		Capped:   true,
		MaxBytes: 1024 * 1024 * 16,
		MaxDocs:  1024,
	})
	if err != nil && strings.Contains(err.Error(), "collection '"+ this.database + "." + this.collection + "' already exists") {
		return nil
	}
	return err
}

func (this *PubSub) StartPubSub() (err chan error) {
	sleep := 100 * time.Millisecond
	t := time.Now()
	tail := this.sess.DB(this.database).C(this.collection).Find(bson.M{}).Tail(10 * time.Second)
	defer func() {
		if e := tail.Close(); e != nil {
			err <- e
		}
	}()

	go func() {
		var message Message
		for {
			time.Sleep(sleep)

			for tail.Next(&message) {
				if message.Timestamp.After(t) {
					for _, ch := range this.events[message.Event] {
						*ch <- message.Body
					}
				}
			}

			if tail.Err() != nil {
				err <- tail.Err()
				_ = tail.Close()
				break
			}

			if tail.Timeout() {
				continue
			}
		}
	}()

	return
}

func (this *PubSub) Publish(event string, body string) error {
	this.sess.SetMode(mgo.Strong, true)
	defer this.sess.SetMode(mgo.SecondaryPreferred, true)
	return this.sess.DB(this.database).C(this.collection).Insert(&Message{
		Event:     event,
		Body:      body,
		Timestamp: time.Now(),
	})
}

func (this *PubSub) Subscribe(event string) *chan string {
	ch := make(chan string)
	this.events[event] = append(this.events[event], &ch)
	return &ch
}

func (this *PubSub) UnSubscribe(event string, ch *chan string) error {
	index := -1
	for i, c := range this.events[event] {
		if ch == c {
			index = i
			break
		}
	}

	if index == -1 {
		return errors.New("pubsub unsubscribe error. maybe leak resources.")
	}

	this.events[event] = append(this.events[event][:index], this.events[event][index+1:]...)
	close(*ch)

	return nil
}