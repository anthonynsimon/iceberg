package iceberg

import (
	"fmt"
	"io"
	"net"
)

type tcpTransport struct {
	ctx *context
}

func (tcpt *tcpTransport) Listen(addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("listening over TCP at", addr)

	for {
		conn, err := lis.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go tcpt.handleConn(conn)
	}
}

func (tcpt *tcpTransport) handleConn(conn net.Conn) {
	var buf [256]byte
	n, err := readFrame(buf[:], conn)
	if err != nil {
		if err != io.EOF {
			fmt.Println(err)
			conn.Close()
			return
		}
	}

	initMessage := buf[:n]

	var protocol *protocolV1
	switch string(initMessage) {
	case "  v1":
		protocol = &protocolV1{
			ctx: tcpt.ctx,
		}
		err = writeFrame([]byte("OK"), conn)
		if err != nil {
			fmt.Println(err)
			return
		}
	default:
		err = writeFrame([]byte("ERR: bad protocol"), conn)
		conn.Close()
		if err != nil {
			fmt.Println(err)
		}
		return
	}

	protocol.handle(conn)
}
