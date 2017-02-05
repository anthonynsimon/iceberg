package main

import (
	"fmt"
	"io"
	"net"
	"strings"
)

func main() {
	pubsub := NewPubSub()

	lis, _ := net.Listen("tcp", ":8080")

	for {
		conn, _ := lis.Accept()
		go handleConn(conn, pubsub)
	}
}

func handleConn(conn net.Conn, pubsub *pubsub) {
	defer conn.Close()

	for {
		var buf [512]byte
		n, err := conn.Read(buf[0:])
		if err != nil {
			if err != io.EOF {
				return
			}
		}
		str := string(buf[0:n])
		var clean []rune
		for _, r := range str {
			if r == '\r' || r == '\n' {
				continue
			}
			clean = append(clean, r)
		}
		str = string(clean)
		index := strings.Index(str, " ")
		if index <= 0 {
			return
		}

		switch str[0:index] {
		case "pub":
			topicEnd := strings.Index(str[index+1:], " ")
			if topicEnd <= 0 {
				return
			}
			topic := str[index+1 : index+1+topicEnd]
			if topic == "" {
				return
			}
			message := str[index+1+topicEnd+1:]
			if message == "" {
				fmt.Println("3")
				return
			}
			pubsub.Publish(topic, message)
			conn.Write([]byte("published on topic: " + topic + "\r\n"))
		case "sub":
			topic := str[index+1:]
			if topic == "" {
				return
			}
			go func() {
				subs := pubsub.Subscribe(topic)
				defer pubsub.Unsubscribe(topic, subs)
				conn.Write([]byte("subscribed to topic: " + topic + "\r\n"))
				for {
					select {
					case msg := <-subs.ch:
						_, err = conn.Write([]byte(msg + "\r\n"))
						if err != nil {
							return
						}
					}
				}
			}()
		}
	}
}

type subscribable struct {
	subscribers []subscriber
}
type subscriber struct {
	ch chan string
}

type pubsub struct {
	topics map[string]*subscribable
}

func NewPubSub() *pubsub {
	return &pubsub{
		topics: make(map[string]*subscribable),
	}
}

func (s *pubsub) Publish(topic, msg string) {
	c, ok := s.topics[topic]
	if !ok {
		c = &subscribable{}
		s.topics[topic] = c
	}
	for _, sub := range c.subscribers {
		sub.ch <- msg
	}
}

func (s *pubsub) Subscribe(topic string) subscriber {
	c, ok := s.topics[topic]
	if !ok {
		c = &subscribable{}
		s.topics[topic] = c
	}
	subs := subscriber{
		ch: make(chan string, 512),
	}
	c.subscribers = append(c.subscribers, subs)
	fmt.Println(len(c.subscribers))
	return subs
}

func (s *pubsub) Unsubscribe(topic string, subs subscriber) {
	t, ok := s.topics[topic]
	if !ok {
		return
	}
	fmt.Println("unsubscribing")
	for i, sub := range t.subscribers {
		if sub == subs {
			t.subscribers = append(t.subscribers[:i], t.subscribers[i+1:]...)
		}
	}
}
