package iceberg

import (
	"encoding/binary"
	"errors"
	"time"
)

const (
	maxMessagePayloadSize = 1000000 // num of bytes
	minMessageSize        = 8 + 16
	maxMessageSize        = minMessageSize + maxMessagePayloadSize
)

// format:
//
// [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
// |        8 byte        |                   16 bytes                    |     n-bytes    |
// -----------------------------------------------------------------------------------------
// |      timestamp       |                      id                       |     payload    |
// |       uint64         |                  hex string                   |      binary    |
//
type message struct {
	timestamp int64
	id        [16]byte
	payload   []byte
}

func newMesage(body []byte) *message {
	return &message{
		// TODO: generate id
		id:        [16]byte{'A', 'B', 'C', 'D', 'E', 'F', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'},
		timestamp: time.Now().UnixNano(),
		payload:   body,
	}
}

func decodeMessage(data []byte) (*message, error) {
	dataLen := len(data)
	if dataLen < minMessageSize {
		return nil, errors.New("message length is too small")
	}
	if dataLen-minMessageSize > maxMessagePayloadSize {
		return nil, errors.New("message payload length is too large")
	}

	msg := message{}
	msg.timestamp = int64(binary.BigEndian.Uint64(data[0:8]))
	copy(msg.id[:], data[8:24])
	msg.payload = data[24:]

	return &msg, nil
}

func encodeMessage(msg *message) ([]byte, error) {
	dataLen := len(msg.payload)
	if dataLen > maxMessagePayloadSize {
		return nil, errors.New("message payload length is too large")
	}

	buf := make([]byte, minMessageSize+dataLen)

	binary.BigEndian.PutUint64(buf[:8], uint64(msg.timestamp))
	copy(buf[8:24], msg.id[:])
	copy(buf[24:], msg.payload)

	return buf, nil
}
