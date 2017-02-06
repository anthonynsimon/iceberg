package iceberg

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type Client interface {
	Connect(addr string) error
	Publish(topic string, msg []byte) error
	Subscribe(topic string, onReceive func(msg []byte)) error
	// Unsubscribe(topic string) error
}

func NewClient(addr string) (Client, error) {
	return &client{}, nil
}

type client struct {
	conn     net.Conn
	shutdown chan bool
	muWrite  sync.Mutex
}

func (cl *client) Shutdown() error {
	fmt.Println("shutting down client")
	cl.shutdown <- true
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
			}
		}
	}()

	cl.conn = conn

	fmt.Println("connected over TCP at", addr)

	_, err = cl.conn.Write([]byte("  v1"))
	if err != nil {
		return err
	}

	var response [2]byte
	_, err = io.ReadFull(cl.conn, response[:])
	if err != nil {
		return err
	}

	fmt.Println("response:", string(response[:]))

	if string(response[:]) != "OK" {
		return errors.New("server did not accept protocol")
	}

	fmt.Println("connection over protocol v1")

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
	_, err = frame.Write([]byte{'\n'})
	if err != nil {
		return err
	}

	bytesFrame := frame.Bytes()

	cl.muWrite.Lock()
	_, err = cl.conn.Write(bytesFrame)
	cl.muWrite.Unlock()
	if err != nil {
		return err
	}

	return nil
}

func (cl *client) Subscribe(topic string, onReceive func(data []byte)) error {
	if cl.conn == nil {
		return errors.New("no open connection")
	}

	fmt.Println("subscribing to topic:", topic)

	frame := bytes.NewBuffer([]byte("sub " + topic))
	_, err := frame.Write([]byte{'\n'})
	if err != nil {
		return err
	}

	bytesFrame := frame.Bytes()

	cl.muWrite.Lock()
	_, err = cl.conn.Write(bytesFrame)
	cl.muWrite.Unlock()

	var response [2]byte
	_, err = io.ReadFull(cl.conn, response[:])
	if err != nil {
		return err
	}

	fmt.Println("response:", string(response[:]))

	reader := bufio.NewReader(cl.conn)
	for {
		select {
		case <-cl.shutdown:
			fmt.Println("shutdown signal received, unsubscribing")
			return nil
		default:
			line, err := reader.ReadSlice('\n')
			if err != nil {
				if err == io.EOF {
					continue
				}
				return err
			}

			line = line[:len(line)-1]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}

			fmt.Println(string(line))

			msg, err := decodeMessage(line[:])
			if err != nil {
				continue
			}
			fmt.Println(time.Unix(0, msg.timestamp))

			fmt.Println("received message with latency", time.Since(time.Unix(0, msg.timestamp)).Nanoseconds()/1000000, "ms")
			onReceive(msg.body)
		}
	}
}
