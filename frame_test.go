package iceberg

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"
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

func TestConcurrentReadWrite(t *testing.T) {
	inputstr := "testingString123456789"
	socketMock := bytes.NewBuffer(nil)
	mu := sync.Mutex{}

	for i := 0; i < 10000; i++ {
		go func(i int) {
			mu.Lock()
			err := writeFrame([]byte(fmt.Sprintf("%s_%04d", inputstr, i)), socketMock)
			mu.Unlock()
			if err != nil {
				t.Error(err)
			}
		}(i)
	}

	results := []string{}
	var readBuf [maxFrameSize]byte
	for i := 0; i < 10000; i++ {
		mu.Lock()
		n, err := readFrame(readBuf[:], socketMock)
		mu.Unlock()
		if err != nil {
			t.Error(err)
		}

		expected := fmt.Sprintf("%s_%04d", inputstr, i)

		if len(readBuf[:n]) != len(expected) {
			t.Errorf("unexpected frame read bytes. expected %d, got %d", len(expected), len(readBuf[:n]))
		}

		result := string(readBuf[:n])

		if !strings.HasPrefix(result, inputstr) {
			t.Errorf("expected %s, got %s", expected, result)
		}

		results = append(results, result)
	}

	sort.Strings(results)
	seen := make(map[string]bool)
	for _, r := range results {
		if seen[r] {
			t.Errorf("found duplicate entry: %s", r)
			break
		}
		seen[r] = true
	}
}
