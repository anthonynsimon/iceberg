package iceberg

import (
	"bytes"
	"fmt"
	"net"
	"sync"
)

const (
	maxFrameSize = maxMessageSize + framePrefixSize
)

type protocolV1 struct {
	ctx *context
	mu  sync.Mutex
}

func (prot *protocolV1) handle(conn net.Conn) {
	defer func() {
		conn.Close()
		fmt.Println("client disconnected", conn.RemoteAddr())
	}()
	fmt.Println("handling protocol v1 connection with", conn.RemoteAddr())

	var buf [maxFrameSize]byte
	for {
		// TODO: add heartbeat
		// TODO: optimize this
		//prot.mu.Lock()
		n, err := readFrame(buf[:], conn)
		//prot.mu.Unlock()

		if err != nil {
			fmt.Println(err)
			return
		}

		line := buf[:n]

		cmdBuf := bytes.NewBuffer(nil)
		topicBuf := bytes.NewBuffer(nil)
		currentBuf := cmdBuf
		cursor := 0
		for cursor < len(line) {
			ch := line[cursor]
			if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '.' {
				currentBuf.WriteByte(ch)
				cursor++
				continue
			}
			if ch == ' ' {
				if currentBuf == cmdBuf && cmdBuf.Len() >= 3 {
					currentBuf = topicBuf
					cursor++
					continue
				} else if currentBuf == topicBuf && topicBuf.Len() >= 1 {
					cursor++
					break
				}
			}
			prot.mu.Lock()
			err := writeFrame([]byte("ERR: malformed command"), conn)
			prot.mu.Unlock()

			if err != nil {
				fmt.Println(err)
			}
			return
		}

		topic := topicBuf.String()

		switch cmdBuf.String() {
		case "pub":
			message := line[cursor:]

			if len(message) < minMessageSize {
				err = writeFrame([]byte("ERR: message length is not valid"), conn)
				if err != nil {
					fmt.Println(err)
				}
				continue
			}

			prot.ctx.stream.Publish(topic, message)

			prot.mu.Lock()
			err := writeFrame([]byte("OK"), conn)
			prot.mu.Unlock()

			if err != nil {
				fmt.Println(err)
				return
			}
		case "sub":
			// fmt.Println("client wants to subscribe to", topic)
			subID, err := prot.ctx.stream.Subscribe(topic, func(msg []byte) {
				prot.mu.Lock()
				err = writeFrame(msg, conn)
				prot.mu.Unlock()
				if err != nil {
					fmt.Println(err)
					return
				}
			})
			if err != nil {
				fmt.Println(err)
				return
			}
			defer prot.ctx.stream.Unsubscribe(topic, subID) // TODO: handle return error?

			prot.mu.Lock()
			err = writeFrame([]byte("OK"), conn)
			prot.mu.Unlock()

			if err != nil {
				fmt.Println(err)
				return
			}
		}
	}
}
