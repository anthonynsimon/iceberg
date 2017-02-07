package iceberg

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

type Client interface {
	Connect(addr string) error
	Publish(topic string, msg []byte) error
	Subscribe(topic string, onReceive func(msg []byte)) error
	Shutdown() error
	// Unsubscribe(topic string) error
}

func NewClient(addr string) (Client, error) {
	return &client{
		shutdown:         make(chan bool, 1),
		shutdownComplete: make(chan bool, 1),
	}, nil
}

type client struct {
	conn             net.Conn
	shutdown         chan bool
	shutdownComplete chan bool
	mu               sync.Mutex
}

func (cl *client) Shutdown() error {
	if cl.conn == nil {
		fmt.Println("can't shutdown, no open connection")
		return errors.New("no open connection")
	}
	fmt.Println("shutting down client")
	cl.shutdown <- true
	<-cl.shutdownComplete
	return nil
}

func (cl *client) Connect(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-cl.shutdown:
				cl.conn.Close()
				cl.conn = nil
				fmt.Println("connection closed")
				cl.shutdownComplete <- true
			}
		}
	}()

	cl.conn = conn

	fmt.Println("connected over TCP at", addr)

	cl.mu.Lock()
	err = writeFrame([]byte("  v1"), conn)
	cl.mu.Unlock()

	if err != nil {
		return err
	}

	var response [512]byte

	cl.mu.Lock()
	n, err := readFrame(response[:], conn)
	cl.mu.Unlock()

	if err != nil {
		return err
	}

	if res := string(response[:n]); res != "OK" {
		return errors.New("server did not accept protocol. response: " + res)
	}

	fmt.Println("using protocol v1")

	return nil
}

func (cl *client) Publish(topic string, data []byte) error {
	if cl.conn == nil {
		return errors.New("no open connection")
	}

	msg := newMesage(data)
	msgEncoded, err := encodeMessage(msg)
	if err != nil {
		return err
	}

	frame := bytes.NewBuffer([]byte("pub " + topic + " "))
	_, err = frame.Write(msgEncoded)
	if err != nil {
		return err
	}

	bytesFrame := frame.Bytes()

	cl.mu.Lock()
	err = writeFrame(bytesFrame, cl.conn)
	cl.mu.Unlock()

	if err != nil {
		return err
	}

	var buf [512]byte

	cl.mu.Lock()
	n, err := readFrame(buf[:], cl.conn)
	cl.mu.Unlock()

	if err != nil {
		return err
	}
	response := buf[:n]

	if res := string(response[:]); res != "OK" {
		return errors.New("error: " + res)
	}

	return nil
}

func (cl *client) Subscribe(topic string, onReceive func(data []byte)) error {
	if cl.conn == nil {
		return errors.New("no open connection")
	}

	cl.mu.Lock()
	err := writeFrame([]byte("sub "+topic), cl.conn)
	cl.mu.Unlock()

	if err != nil {
		return err
	}

	var response [512]byte

	cl.mu.Lock()
	n, err := readFrame(response[:], cl.conn)
	cl.mu.Unlock()

	if err != nil {
		return err
	}

	if res := string(response[:n]); res != "OK" {
		return errors.New("error: " + res)
	}

	var readBuf [maxMessageSize]byte
	for {
		select {
		case <-cl.shutdown:
			fmt.Println("shutdown signal received, unsubscribing")
			return nil
		default:
			cl.mu.Lock()
			n, err := readFrame(readBuf[:], cl.conn)
			cl.mu.Unlock()

			if err != nil {
				if err == io.EOF {
					continue
				}
				return err
			}

			message := readBuf[:n]

			decodedMessage, err := decodeMessage(message[:])
			if err != nil {
				continue
			}

			//fmt.Println("received message. latency:", time.Since(time.Unix(0, decodedMessage.timestamp)).Nanoseconds()/1000000, "ms")
			onReceive(decodedMessage.payload)
		}
	}
}
