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

package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

type Endpoint func(in *Message) (out *Message)

// Magic Codes
type MagicCode uint8

const (
	MagicReq MagicCode = 0xE2
	MagicRes MagicCode = 0xE3
)

type OpCode uint8

// ops
const (
	OpExPut OpCode = OpCode(iota)
)

type StatusCode uint8

// error codes
const (
	StatusOK = StatusCode(iota)
	StatusUnknownEndpoint
	StatusInternalServerError
)

var (
	ErrUnknownEndpoint     = errors.New("unknown endpoint")
	ErrInternalServerError = errors.New("internal server error")
)

const HeaderSize int = 12

// total length    // 12
type Header struct {
	Magic    MagicCode  // 1
	Op       OpCode     // 1
	DMapLen  uint16     // 2
	KeyLen   uint16     // 2
	ExtraLen uint8      // 1
	Status   StatusCode // 1
	BodyLen  uint32     // 4
}

type Message struct {
	Header               // [0..10]
	Extras []interface{} // [11..(m-1)] Command specific extras (In)
	DMap   string        // [m..(n-1)] DMap (as needed, length in Header)
	Key    []byte        // [n..(x-1)] Key (as needed, length in Header)
	Value  []byte        // [x..y] Value (as needed, length in Header)
}

func (m *Message) Read(conn net.Conn, b []byte) error {
	_, err := conn.Read(b)
	if err != nil {
		return err
	}
	err = binary.Read(bytes.NewReader(b), binary.BigEndian, &m.Header)
	if err != nil {
		return err
	}

	if m.Magic != MagicReq && m.Magic != MagicRes {
		return fmt.Errorf("invalid message")
	}

	// TODO: Use a pool for this.
	bd := make([]byte, m.BodyLen)
	_, err = conn.Read(bd)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(bd)
	m.DMap = string(buf.Next(int(m.DMapLen)))
	m.Key = buf.Next(int(m.KeyLen))
	vlen := int(m.BodyLen) - int(m.ExtraLen) - int(m.KeyLen) - int(m.DMapLen)
	m.Value = buf.Next(int(vlen))
	return nil
}

func (m *Message) Write(conn net.Conn, buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.BigEndian, m.Header)
	if err != nil {
		return err
	}
	for _, e := range m.Extras {
		err = binary.Write(buf, binary.BigEndian, e)
		if err != nil {
			return err
		}
	}
	_, err = buf.WriteString(m.DMap)
	if err != nil {
		return err
	}

	_, err = buf.Write(m.Key)
	if err != nil {
		return err
	}

	_, err = buf.Write(m.Value)
	if err != nil {
		return err
	}

	_, err = buf.WriteTo(conn)
	return err
}

// SizeOfExtras returns the size of the extras field for the memcache request.
func SizeOfExtras(extras []interface{}) (l uint8) {
	for _, e := range extras {
		switch e.(type) {
		default:
			panic(fmt.Sprintf("transport/client: unknown extra type (%T)", e))
		case uint8:
			l += 8 / 8
		case uint16:
			l += 16 / 8
		case uint32:
			l += 32 / 8
		case uint64:
			l += 64 / 8
		}
	}
	return
}

func CheckError(resp *Message) error {
	switch {
	case resp.Status == StatusOK:
		return nil
	case resp.Status == StatusUnknownEndpoint:
		return ErrUnknownEndpoint
	case resp.Status == StatusInternalServerError:
		return ErrInternalServerError
	}
	return fmt.Errorf("Unknown status code: %d", resp.Status)
}

func (m *Message) Error(status StatusCode, err interface{}) *Message {
	var n Message
	switch err.(type) {
	case string:
		n.Value = []byte(err.(string))
	case error:
		n.Value = []byte(err.(error).Error())
	}
	n.Magic = MagicRes
	n.Op = m.Op
	n.Status = status
	n.BodyLen = uint32(len(n.Value))
	return &n
}
