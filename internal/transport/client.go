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
	"encoding/binary"
	"net"

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

func (c *Client) SendMsg(m *msg) (*msg, error) {
	buf := c.bufpool.Get()
	defer c.bufpool.Put(buf)

	m.Magic = magicSend
	m.Op = OpExPut
	m.DMapLen = uint16(len(m.dmap))
	m.KeyLen = uint16(len(m.key))
	m.ExtraLen = 0
	m.BodyLen = uint32(len(m.dmap) + len(m.key) + len(m.value) + int(m.ExtraLen))
	err := binary.Write(buf, binary.BigEndian, m.header)
	if err != nil {
		return nil, err
	}
	for _, e := range m.extras {
		err = binary.Write(buf, binary.BigEndian, e)
		if err != nil {
			return nil, err
		}
	}
	_, err = buf.WriteString(m.dmap)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(m.value)
	if err != nil {
		return nil, err
	}

	conn, err := c.pool.Get()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	_, err = buf.WriteTo(conn)
	if err != nil {
		return nil, err
	}
	// TODO: Wait for response
	return nil, nil
}
