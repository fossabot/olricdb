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

// Magic Codes
type magicCode uint8

const (
	magicSend magicCode = 0xE2
	magicRes  magicCode = 0xE3
)

type opCode uint8

// ops
const (
	OpExPut opCode = opCode(iota)
)

// total length    // 11
type header struct {
	Magic    magicCode // 1
	Op       opCode    // 1
	DMapLen  uint16    // 2
	KeyLen   uint16    // 2
	ExtraLen uint8     // 1
	BodyLen  uint32    // 4
}

type msg struct {
	header               // [0..10]
	extras []interface{} // [11..(m-1)] Command specific extras (In)
	dmap   string        // [m..(n-1)] DMap (as needed, length in header)
	key    []byte        // [n..(x-1)] Key (as needed, length in header)
	value  []byte        // [x..y] Value (as needed, length in header)
}
