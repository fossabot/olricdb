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
	"net"

	"github.com/buraksezer/olricdb/protocol"
	"github.com/buraksezer/pool"
)

type Client struct {
	addr    string
	pool    pool.Pool
	bufpool *BufPool
}

// create a factory() to be used with channel based pool
func (c *Client) factory() (net.Conn, error) {
	return net.Dial("tcp", c.addr)
}

func NewClient(addr string, minConn, maxConn int) (*Client, error) {
	c := &Client{
		addr:    addr,
		bufpool: NewBufPool(),
	}
	p, err := pool.NewChannelPool(minConn, maxConn, c.factory)
	if err != nil {
		return nil, err
	}
	c.pool = p
	return c, nil
}

func (c *Client) Close() {
	c.pool.Close()
}

func (c *Client) Request(op protocol.OpCode, m *protocol.Message) (*protocol.Message, error) {
	buf := c.bufpool.Get()
	defer c.bufpool.Put(buf)

	m.Magic = protocol.MagicReq
	m.Op = op
	m.DMapLen = uint16(len(m.DMap))
	m.KeyLen = uint16(len(m.Key))
	m.ExtraLen = protocol.SizeOfExtras(m.Extras)
	m.BodyLen = uint32(len(m.DMap) + len(m.Key) + len(m.Value) + int(m.ExtraLen))

	conn, err := c.pool.Get()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	err = m.Write(conn, buf)
	if err != nil {
		return nil, err
	}

	var resp protocol.Message
	header := make([]byte, 12)
	err = resp.Read(conn, header)
	if err != nil {
		return nil, err
	}
	err = protocol.CheckError(&resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}
