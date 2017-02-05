package iceberg

import (
	"errors"

	"sync"

	trie "github.com/armon/go-radix"
)

type Iceberg interface {
	Publish(topic string, msg []byte) error
	Subscribe(topic string, onReceive func(msg []byte)) (uint64, error)
	Unsubscribe(topic string, id uint64) error
}

type icequeue struct {
	// TODO:
	// - switch to a CTrie
	// - handle concurrency!
	topics *trie.Tree
	idSeq  uint64
	mutex  sync.Mutex
}

func newIcequeue() *icequeue {
	return &icequeue{
		topics: trie.New(),
		idSeq:  0,
	}
}

type subscriber struct {
	ID        uint64
	OnReceive func(msg []byte)
}

func (q *icequeue) Publish(topic string, msg []byte) error {
	q.mutex.Lock()
	t, ok := q.topics.Get(topic)
	q.mutex.Unlock()
	if !ok {
		t = []subscriber{}
		q.topics.Insert(topic, t)
	}

	subscribers := t.([]subscriber)
	for _, subs := range subscribers {
		subs.OnReceive(msg)
	}

	return nil
}

func (q *icequeue) Subscribe(topic string, onReceive func(msg []byte)) (uint64, error) {
	q.mutex.Lock()
	t, ok := q.topics.Get(topic)
	q.mutex.Unlock()
	if !ok {
		t = []subscriber{}
		q.mutex.Lock()
		q.topics.Insert(topic, t)
		q.mutex.Unlock()
	}

	subscribers := t.([]subscriber)
	subscribers = append(subscribers, subscriber{
		ID:        q.idSeq + 1,
		OnReceive: onReceive,
	})

	q.mutex.Lock()
	q.topics.Insert(topic, subscribers)
	q.mutex.Unlock()

	q.idSeq++

	return q.idSeq, nil
}

func (q *icequeue) Unsubscribe(topic string, id uint64) error {
	q.mutex.Lock()
	t, ok := q.topics.Get(topic)
	q.mutex.Unlock()
	if !ok {
		return errors.New("no subscribers for topic")
	}

	subscribers := t.([]subscriber)
	for i, sub := range subscribers {
		if sub.ID == id {
			subscribers = append(subscribers[0:i], subscribers[i+1:]...)
			q.mutex.Lock()
			q.topics.Insert(topic, subscribers)
			q.mutex.Unlock()
			return nil
		}
	}

	return errors.New("id not found")
}
