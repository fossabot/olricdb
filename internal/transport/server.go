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
	"io"
	"log"
	"net"
	"os"
	"sync"

	"github.com/buraksezer/olricdb/protocol"
	"github.com/pkg/errors"
)

type operations struct {
	m map[protocol.OpCode]protocol.Operation
}

// Server implements a TCP server.
type Server struct {
	addr       string
	bufpool    *BufPool
	operations operations
	logger     *log.Logger
	wg         sync.WaitGroup
	listener   *net.TCPListener
	connCh     chan net.Conn
	StartCh    chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewServer(addr string, logger *log.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	return &Server{
		operations: operations{m: make(map[protocol.OpCode]protocol.Operation)},
		addr:       addr,
		bufpool:    NewBufPool(),
		logger:     logger,
		connCh:     make(chan net.Conn),
		StartCh:    make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (s *Server) RegisterOperation(op protocol.OpCode, e protocol.Operation) {
	s.operations.m[op] = e
}

func (s *Server) processMessage(m *protocol.Message, conn net.Conn, header []byte) error {
	buf := s.bufpool.Get()
	defer s.bufpool.Put(buf)

	err := m.Read(conn, header)
	if err == io.EOF || err == protocol.ErrConnClosed {
		return err
	}
	if err != nil {
		return err
	}

	opr, ok := s.operations.m[m.Op]
	if !ok {
		return protocol.ErrUnknownOperation
	}
	resp := opr(m)
	err = resp.Write(conn, buf)
	if err == protocol.ErrConnClosed {
		return err
	}
	return errors.WithMessage(err, "failed to write response")
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
	// TODO: Add Deadline to close idle sockets
	header := make([]byte, protocol.HeaderSize)
	for {
		var req protocol.Message
		err := s.processMessage(&req, conn, header)
		if err == nil {
			continue
		}
		if err == io.EOF || err == protocol.ErrConnClosed {
			break
		}

		var status protocol.StatusCode
		if err == protocol.ErrUnknownOperation {
			status = protocol.StatusUnknownOperation
		} else {
			status = protocol.StatusInternalServerError
		}

		resp := req.Error(status, err)
		buf := s.bufpool.Get()
		defer s.bufpool.Put(buf)
		err = resp.Write(conn, buf)
		if err != nil && err != protocol.ErrConnClosed {
			s.logger.Printf("[ERROR] Failed to return error message: %v", err)
		}
		break
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
	// TODO: Temporary hack to fix tests, remove this after removing HTTP
	if s.listener != nil {
		err := s.listener.Close()
		s.wg.Wait()
		return err
	}
	return nil
}
