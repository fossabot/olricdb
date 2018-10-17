// Copyright 2018 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
)

// Server implements a TCP server.
type Server struct {
	addr     string
	logger   *log.Logger
	wg       sync.WaitGroup
	listener *net.TCPListener
	connCh   chan net.Conn
	StartCh  chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewServer(addr string, logger *log.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	return &Server{
		addr:    addr,
		logger:  logger,
		connCh:  make(chan net.Conn),
		StartCh: make(chan struct{}),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()

	done := make(chan struct{})
	defer close(done)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case <-s.ctx.Done():
		case <-done:
		}

		if err := conn.Close(); err != nil {
			s.logger.Printf("[DEBUG] Failed to close TCP connection: %v", err)
		}
	}()

	b := make([]byte, 8192)
	for {
		nr, err := conn.Read(b)
		if err != nil {
			s.logger.Printf(
				"[DEBUG] Failed to read from TCP connection. Addr: %s, %v",
				conn.RemoteAddr(), err)
			return
		}
		fmt.Println("READ FROM SOCKET: ", string(b), nr)
	}
}

func (s *Server) handleConns() {
	defer s.wg.Done()

	for {
		select {
		case conn := <-s.connCh:
			s.wg.Add(1)
			go s.handleConn(conn)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Server) ListenAndServe() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		close(s.StartCh)
		return err
	}
	s.listener = l.(*net.TCPListener)

	s.wg.Add(1)
	go s.handleConns()
	close(s.StartCh)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return nil
			default:
			}
			s.logger.Printf("[DEBUG] Failed to accept TCP connection: %v", err)
			continue
		}
		s.connCh <- conn
	}
	return nil
}

func (s *Server) Shutdown() error {
	s.cancel()
	err := s.listener.Close()
	s.wg.Wait()
	return err
}
