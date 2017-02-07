package iceberg

import (
	"fmt"
	"net"
)

type Server struct {
	addr     string
	ctx      *context
	shutdown chan bool
}

func NewServer(addr string) (*Server, error) {
	_, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Server{
		addr:     addr,
		shutdown: make(chan bool, 1),
	}, nil
}

type context struct {
	stream *stream
}

func (srv *Server) Run() {
	ctx := &context{
		stream: newStream(),
	}

	tcp := newTCPTransport(ctx)

	go func() {
		tcp.Listen(srv.addr)
	}()

	for {
		select {
		case <-srv.shutdown:
			tcp.Shutdown()
			return
		default:
			continue
		}
	}
}

func (src *Server) Shutdown() {
	fmt.Println("shutting down server")
	src.shutdown <- true
}
