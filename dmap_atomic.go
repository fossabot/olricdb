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
	"fmt"
	"reflect"
	"time"
)

func (db *OlricDB) atomicIncrDecr(name, key, opr string, delta int) (int, error) {
	rawval, err := db.get(name, key)
	if err != nil && err != ErrKeyNotFound {
		return 0, err
	}

	var newval, curval int
	if err == ErrKeyNotFound {
		err = nil
	} else {
		var value interface{}
		if err = db.serializer.Unmarshal(rawval, &value); err != nil {
			return 0, err
		}
		// switch is faster than reflect.
		switch value.(type) {
		case int:
			curval = value.(int)
		default:
			return 0, fmt.Errorf("mismatched type: %v", reflect.TypeOf(value).Name())
		}

	}

	if opr == "incr" {
		newval = curval + delta
	} else if opr == "decr" {
		newval = curval - delta
	} else {
		return 0, fmt.Errorf("invalid operation")
	}

	err = db.put(name, key, newval, nilTimeout)
	if err != nil {
		return 0, err
	}
	return newval, nil
}

// Incr atomically increments key by delta. The return value is the new value after being incremented or an error.
func (dm *DMap) Incr(key string, delta int) (int, error) {
	err := dm.LockWithTimeout(key, time.Minute)
	if err != nil {
		return 0, err
	}
	defer func() {
		err = dm.Unlock(key)
		if err != nil {
			dm.db.logger.Printf("[ERROR] Failed to release the lock for key: %s", key)
		}
	}()
	return dm.db.atomicIncrDecr(dm.name, key, "incr", delta)
}

// Decr atomically decrements key by delta. The return value is the new value after being decremented or an error.
func (dm *DMap) Decr(key string, delta int) (int, error) {
	err := dm.LockWithTimeout(key, time.Minute)
	if err != nil {
		return 0, err
	}
	defer func() {
		err = dm.Unlock(key)
		if err != nil {
			dm.db.logger.Printf("[ERROR] Failed to release the lock for key: %s", key)
		}
	}()
	return dm.db.atomicIncrDecr(dm.name, key, "decr", delta)
}
