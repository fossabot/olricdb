package transport

import (
	"net"
	"testing"

	"github.com/buraksezer/olricdb/protocol"
)

func newTestServer() (*Server, func() net.Addr) {
	server := NewServer("localhost:0", nil)
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			server.logger.Printf("Failed to run TCP server: %v", err)
		}
	}()
	<-server.StartCh
	return server, server.listener.Addr
}

func TestMessaging_Put(t *testing.T) {
	server, addr := newTestServer()
	defer func() {
		err := server.Shutdown()
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	}()
	c, err := NewClient(addr().String(), 1, 1)
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}

	bkey := []byte("mykey")
	value := []byte("myvalue")
	m := &protocol.Message{
		DMap:  "mydmap",
		Key:   bkey,
		Value: value,
	}
	_, err = c.Request(protocol.OpExPut, m)
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}
}