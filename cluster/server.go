package cluster

import (
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
)

type Server struct {
	name    string
	network string
	address string

	listener net.Listener
	handler  func(conn io.ReadWriteCloser)
}

func (s *Server) RegisterName(name string, rcvr interface{}) error {
	if err := rpc.RegisterName(name, rcvr); err != nil {
		return err
	}

	s.handler = func(conn io.ReadWriteCloser) {
		rpc.ServeConn(conn)
	}
	return nil
}

func (s *Server) Run() error {
	errChan := make(chan error, 1)
	go func() { errChan <- s.Start() }()

	if err := s.waitSignal(errChan); err != nil {
		log.Fatal("received error and exit: ", err.Error())
		return err
	}

	// stop server after user hooks
	if err := s.Stop(); err != nil {
		log.Fatal("stop server error: ", err.Error())
		return err
	}
	return nil
}

func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen(s.network, s.address)
	if err != nil {
		log.Fatal("Server ListenTCP error:", err)
		return err
	}
	log.Printf("%s Server Started.", s.name)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Fatal("Accept error:", err)
			return err
		}
		s.handler(conn)
	}
}

func (s *Server) waitSignal(errCh chan error) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGHUP, syscall.SIGTERM)

	for {
		select {
		case sig := <-signals:
			switch sig {
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM:
				return nil
			}
		case err := <-errCh:
			return err
		}
	}
}

func (s *Server) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
