package iceberg

import (
	"fmt"
	"io"
	"net"
)

const (
	protocolV1Ident = "  v1"
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
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		if err != io.EOF {
			fmt.Println(err)
			conn.Close()
			return
		}
	}

	var protocol *protocolV1
	switch string(buf) {
	case protocolV1Ident:
		protocol = &protocolV1{
			ctx: tcpt.ctx,
		}
		conn.Write([]byte("OK"))
	default:
		conn.Write([]byte("bad protocol\n\r"))
		conn.Close()
		return
	}

	protocol.handle(conn)
}
