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

import "github.com/buraksezer/olricdb/protocol"

func (db *OlricDB) exPutOperation(req *protocol.Message) *protocol.Message {
	err := db.put(req.DMap, string(req.Key), req.Value, nilTimeout)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	var resp protocol.Message
	resp.Magic = protocol.MagicRes
	resp.Op = req.Op
	resp.Status = protocol.StatusOK
	return &resp
}
