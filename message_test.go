package iceberg

import "testing"

func TestMessageEncodeDecode(t *testing.T) {
	cases := []*message{
		newMesage([]byte("hello\r\n")),
		newMesage([]byte("hello")),
		newMesage([]byte("testing something in here")),
		newMesage([]byte{10, 20, 40, 50, 60, 70, 80, 90, 100}),
	}

	for _, msg := range cases {
		encoded, err := encodeMessage(msg)
		if err != nil {
			t.Error(err)
		}
		decoded, err := decodeMessage(encoded)
		if err != nil {
			t.Error(err)
		}
		if decoded.id != msg.id {
			t.Errorf("\nexpected id: %v\nactual: %v", msg.id, decoded.id)
		}
		if decoded.timestamp != msg.timestamp {
			t.Errorf("\nexpected timestamp: %v\nactual: %v", msg.timestamp, decoded.timestamp)
		}
		if len(decoded.body) != len(msg.body) {
			t.Errorf("\nexpected body length: %v\nactual: %v", len(msg.body), len(decoded.body))
		}
		for i := range decoded.body {
			if decoded.body[i] != msg.body[i] {
				t.Errorf("\nexpected body: %v\nactual: %v", msg.body, decoded.body)
				break
			}
		}
	}
}
