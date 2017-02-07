package iceberg

import (
	"encoding/binary"
	"io"
)

const (
	framePrefixSize = 4
)

func writeFrame(frame []byte, w io.Writer) error {
	frameLen := len(frame)
	buf := make([]byte, frameLen+framePrefixSize)
	binary.BigEndian.PutUint32(buf[0:framePrefixSize], uint32(frameLen))
	copy(buf[framePrefixSize:], frame[:])
	_, err := w.Write(buf)
	return err
}

func readFrame(dst []byte, r io.Reader) (int, error) {
	var prefix [framePrefixSize]byte
	_, err := io.ReadFull(r, prefix[:])
	if err != nil {
		if err != io.EOF {
			return -1, err
		}
	}

	frameLen := int(binary.BigEndian.Uint32(prefix[:]))
	n, err := io.LimitReader(r, maxMessageSize).Read(dst[0:frameLen])
	if err != nil {
		if err != io.EOF {
			return -1, err
		}
	}

	return n, nil
}
