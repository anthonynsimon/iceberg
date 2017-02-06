package iceberg

import (
	"encoding/binary"
	"errors"
	"time"
)

const (
	maxMessageBodyBytes = 1024
	minMessageBytes     = 8 + 16
)

// format:
//
// [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
// |        8 byte        |                   16 bytes                    |     n-bytes    |
// -----------------------------------------------------------------------------------------
// |      timestamp       |                      id                       |      body      |
// |       uint64         |                  hex string                   |     binary     |
//
type message struct {
	timestamp int64
	id        [16]byte
	body      []byte
}

func newMesage(body []byte) *message {
	return &message{
		id:        [16]byte{'A', 'B', 'C', 'D', 'E', 'F', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'},
		timestamp: time.Now().UnixNano(),
		body:      body,
	}
}

func decodeMessage(data []byte) (*message, error) {
	dataLen := len(data)
	if dataLen < minMessageBytes {
		return nil, errors.New("message length is too small")
	}
	if dataLen-minMessageBytes > maxMessageBodyBytes {
		return nil, errors.New("message body length is too large")
	}

	msg := message{}
	msg.timestamp = int64(binary.BigEndian.Uint64(data[0:8]))
	copy(msg.id[:], data[8:24])
	msg.body = data[24:]

	return &msg, nil
}

func encodeMessage(msg *message) ([]byte, error) {
	dataLen := len(msg.body)
	if dataLen > maxMessageBodyBytes {
		return nil, errors.New("message body length is too large")
	}

	buf := make([]byte, minMessageBytes+dataLen)

	binary.BigEndian.PutUint64(buf[:8], uint64(msg.timestamp))
	copy(buf[8:24], msg.id[:])
	copy(buf[24:], msg.body)

	return buf, nil
}
