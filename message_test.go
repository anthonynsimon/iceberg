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
		if len(decoded.payload) != len(msg.payload) {
			t.Errorf("\nexpected payload length: %v\nactual: %v", len(msg.payload), len(decoded.payload))
		}
		for i := range decoded.payload {
			if decoded.payload[i] != msg.payload[i] {
				t.Errorf("\nexpected payload: %v\nactual: %v", msg.payload, decoded.payload)
				break
			}
		}
	}
}
