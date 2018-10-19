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

package olricdb

import (
	"context"
	"testing"

	"github.com/buraksezer/olricdb/internal/transport"
	"github.com/buraksezer/olricdb/protocol"
	"github.com/hashicorp/memberlist"
)

const testTCPServerAddr = "localhost:46523"

func newTestServer(peers []string) (*OlricDB, error) {
	mc := memberlist.DefaultLocalConfig()
	mc.BindPort = 0
	mc.Name = testTCPServerAddr
	cfg := &Config{
		PartitionCount:   7,
		BackupCount:      1,
		Name:             mc.Name,
		Peers:            peers,
		MemberlistConfig: mc,
	}
	db, err := New(cfg)
	if err != nil {
		return nil, err
	}

	server := transport.NewServer(testTCPServerAddr, nil)
	db.server = server
	go db.listenAndServe()
	<-server.StartCh

	err = db.prepare()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func TestExternal_UnknownOperation(t *testing.T) {
	db, err := newTestServer(nil)
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()
	c, err := transport.NewClient(testTCPServerAddr, 1, 1)
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}

	m := &protocol.Message{
		DMap:  "mydmap",
		Key:   []byte("mykey"),
		Value: []byte("myvalue"),
	}
	_, err = c.Request(protocol.OpCode(255), m)
	if err != protocol.ErrUnknownOperation {
		t.Errorf("Expected ErrUnknownOperation. Got: %v", err)
	}
}

func TestExternal_Put(t *testing.T) {
	db, err := newTestServer(nil)
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	c, err := transport.NewClient(testTCPServerAddr, 1, 1)
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}

	m := &protocol.Message{
		DMap:  "mydmap",
		Key:   []byte("mykey"),
		Value: []byte("myvalue"),
	}
	resp, err := c.Request(protocol.OpExPut, m)
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}
	err = protocol.CheckError(resp)
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}
}
