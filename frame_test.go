package iceberg

import (
	"bytes"
	"testing"
)

func TestReadWriteFrame(t *testing.T) {
	inputstr := "hello\n"
	frame := bytes.NewBuffer(nil)
	err := writeFrame([]byte(inputstr), frame)
	if err != nil {
		t.Error(err)
	}
	frameBytes := frame.Bytes()
	expectedBytes := []byte{0, 0, 0, 6, 104, 101, 108, 108, 111, 10}

	if len(frameBytes) != len(expectedBytes) {
		t.Error("unexpected frame bytes length")
	}

	for i := range frameBytes {
		if frameBytes[i] != expectedBytes[i] {
			t.Errorf("unexpected frame byte at index %d", i)
		}
	}

	var readBuf [512]byte
	n, err := readFrame(readBuf[:], frame)
	if err != nil {
		t.Error(err)
	}

	if n != len(inputstr) {
		t.Error("unexpected frame read n")
	}

	result := string(readBuf[:n])

	if result != inputstr {
		t.Errorf("expected %s, got %s", inputstr, result)
	}
}

/*
func TestConcurrentReadWrite(t *testing.T) {
	inputstr := "hello"
	writeBuf := bytes.NewBuffer(nil)
	writeChan := make(chan []byte, 1)
	readChan := make(chan io.Reader, 1)

	go func(wr io.ReadWriter) {
		for {
			select {
			case b := <-writeChan:
				err := writeFrame(b, wr)
				if err != nil {
					t.Error(err)
				}
				readChan <- wr
			}
		}
	}(writeBuf)

	for i := 0; i < 1000; i++ {
		go func(i int) {
			writeChan <- []byte(fmt.Sprintf("%s_%04d", inputstr, i))
		}(i)
	}

	for i := 0; i < 1000; i++ {
		read := <-readChan
		readBuf, err := readFrame(read)
		if err != nil {
			t.Error(err)
		}

		expected := fmt.Sprintf("%s_%04d", inputstr, i)

		if len(readBuf) != len(expected) {
			t.Errorf("unexpected frame read bytes. expected %d, got %d", len(expected), len(readBuf))
		}

		result := string(readBuf)

		if !strings.HasPrefix(result, inputstr) {
			t.Errorf("expected %s, got %s", expected, result)
		}

		fmt.Println(result)
	}
}
*/
