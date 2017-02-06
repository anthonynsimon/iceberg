package iceberg

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
)

type protocolV1 struct {
	ctx *context
}

func (prot *protocolV1) handle(conn net.Conn) {
	defer func() {
		conn.Close()
		fmt.Println("client disconnected", conn.RemoteAddr())
	}()
	fmt.Println("handling protocol v1 connection with", conn.RemoteAddr())

	reader := bufio.NewReader(conn)
	for {
		// TODO: add heartbeat
		// TODO: optimize this
		line, err := reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				continue
			}
			fmt.Println(err)
			return
		}

		line = line[:len(line)-1]
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}

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
			conn.Write([]byte("malformed command\r\n"))
			return
		}

		topic := topicBuf.String()

		switch cmdBuf.String() {
		case "pub":
			message := line[cursor:]
			if len(message) < minMessageBytes {
				conn.Write([]byte("ERR\r\n"))
				fmt.Println("message length is not valid")
				continue
			}

			fmt.Println("client wants to publish to", topic)
			fmt.Println(string(message))
			prot.ctx.stream.Publish(topic, message)
			conn.Write([]byte("OK\r\n"))
		case "sub":
			fmt.Println("client wants to subscribe to", topic)
			wl := sync.Mutex{}
			subID, err := prot.ctx.stream.Subscribe(topic, func(msg []byte) {
				frame := bytes.NewBuffer(msg)
				frame.Write([]byte{'\n'})
				frameBytes := frame.Bytes()
				wl.Lock()
				_, err = conn.Write(frameBytes)
				wl.Unlock()
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
			conn.Write([]byte("OK"))
			fmt.Println("subscribed")
		}
	}
}
