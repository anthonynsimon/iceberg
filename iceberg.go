package iceberg

import (
	"errors"

	"github.com/Workiva/go-datastructures/trie/ctrie"
)

type Stream interface {
	Publish(topic string, msg []byte) error
	Subscribe(topic string, onReceive func(msg []byte)) (uint64, error)
	Unsubscribe(topic string, id uint64) error
}

type stream struct {
	topics *ctrie.Ctrie
	idSeq  uint64
}

func newStream() *stream {
	return &stream{
		topics: ctrie.New(nil),
		idSeq:  0,
	}
}

type subscriber struct {
	ID        uint64
	OnReceive func(msg []byte)
}

func (q *stream) Publish(topic string, msg []byte) error {
	t, ok := q.topics.Lookup([]byte(topic))
	if !ok {
		t = []subscriber{}
		q.topics.Insert([]byte(topic), t)
	}

	subscribers := t.([]subscriber)
	for _, subs := range subscribers {
		subs.OnReceive(msg)
	}

	return nil
}

func (q *stream) Subscribe(topic string, onReceive func(msg []byte)) (uint64, error) {
	t, ok := q.topics.Lookup([]byte(topic))
	if !ok {
		t = []subscriber{}
		q.topics.Insert([]byte(topic), t)
	}

	subscribers := t.([]subscriber)
	subscribers = append(subscribers, subscriber{
		ID:        q.idSeq + 1,
		OnReceive: onReceive,
	})

	q.topics.Insert([]byte(topic), subscribers)

	q.idSeq++

	return q.idSeq, nil
}

func (q *stream) Unsubscribe(topic string, id uint64) error {
	t, ok := q.topics.Lookup([]byte(topic))
	if !ok {
		return errors.New("no subscribers for topic")
	}

	subscribers := t.([]subscriber)
	for i, sub := range subscribers {
		if sub.ID == id {
			subscribers = append(subscribers[0:i], subscribers[i+1:]...)
			q.topics.Insert([]byte(topic), subscribers)
			return nil
		}
	}

	return errors.New("id not found")
}
