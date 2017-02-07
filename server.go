package iceberg

import "sync"

type Server struct {
	ctx *context
}

type context struct {
	stream *stream
}

func (srv *Server) Run() {
	ctx := &context{
		stream: newStream(),
	}

	tcp := tcpTransport{
		ctx: ctx,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		tcp.Listen(":7260")
	}()
	wg.Wait()
}
