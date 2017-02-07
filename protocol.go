package iceberg

import (
	"bytes"
	"encoding/binary"
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

	var buf [2048]byte
	for {
		// TODO: add heartbeat
		// TODO: optimize this
		n, err := readFrame(buf[:], conn)
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
			err := writeFrame([]byte("ERR: malformed command"), conn)
			if err != nil {
				fmt.Println(err)
			}
			return
		}

		topic := topicBuf.String()

		switch cmdBuf.String() {
		case "pub":
			message := line[cursor:]

			if len(message) < minMessageBytes {
				fmt.Println("line", string(line))
				fmt.Println("cmd", cmdBuf.String())
				fmt.Println("topic", topic)
				fmt.Println("cursor", cursor)
				fmt.Println("\\r\\n bytes", []byte{'\r', '\n'})
				fmt.Println("message bytes", message)
				fmt.Println("line bytes", line)
				fmt.Print("START_START::" + string(message) + "::END_END\n")

				conn.Write([]byte("ERR: message length is not valid"))
				continue
			}

			prot.ctx.stream.Publish(topic, message)

			err := writeFrame([]byte("OK"), conn)
			if err != nil {
				fmt.Println(err)
				return
			}
		case "sub":
			// fmt.Println("client wants to subscribe to", topic)
			wl := sync.Mutex{}
			subID, err := prot.ctx.stream.Subscribe(topic, func(msg []byte) {
				wl.Lock()
				err = writeFrame(msg, conn)
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
			wl.Lock()
			err = writeFrame([]byte("OK"), conn)
			if err != nil {
				fmt.Println(err)
				return
			}
			wl.Unlock()
		}
	}
}

func writeFrame(frame []byte, w io.Writer) error {
	frameLen := len(frame)
	buf := make([]byte, frameLen+8)
	binary.BigEndian.PutUint64(buf[0:8], uint64(frameLen))
	copy(buf[8:], frame[:])
	_, err := w.Write(buf)
	return err
}

func readFrame(dst []byte, r io.Reader) (int, error) {
	_, err := io.ReadFull(r, dst[0:8])
	if err != nil {
		if err != io.EOF {
			return 0, err
		}
	}

	n := int(binary.BigEndian.Uint64((dst[:8])))
	fmt.Println(string(dst[:8]))
	_, err = io.ReadFull(r, dst[0:n])
	if err != nil {
		if err != io.EOF {
			return 0, err
		}
	}

	return n, nil
}
