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
	"sync"
	"testing"
)

func TestDMap_Incr(t *testing.T) {
	r, srv, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv.Close()
	defer func() {
		err = r.Shutdown(context.Background())
		if err != nil {
			r.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	var wg sync.WaitGroup
	var start chan struct{}
	key := "incr"

	incr := func(dm *DMap, i int) {
		<-start
		defer wg.Done()

		_, err := dm.Incr(key, 1)
		if err != nil {
			r.logger.Printf("[ERROR] Failed to call Incr: %v", err)
			return
		}
	}

	dm := r.NewDMap("atomic_test")
	start = make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go incr(dm, i)
	}
	close(start)
	wg.Wait()

	res, err := dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if res.(int) != 100 {
		t.Fatalf("Expected 100. Got: %v", res)
	}
}

func TestDMap_Decr(t *testing.T) {
	r, srv, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv.Close()
	defer func() {
		err = r.Shutdown(context.Background())
		if err != nil {
			r.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	var wg sync.WaitGroup
	var start chan struct{}
	key := "decr"

	decr := func(dm *DMap, i int) {
		<-start
		defer wg.Done()

		_, err := dm.Decr(key, 1)
		if err != nil {
			r.logger.Printf("[ERROR] Failed to call Decr: %v", err)
			return
		}
	}

	dm := r.NewDMap("atomic_test")
	start = make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go decr(dm, i)
	}
	close(start)
	wg.Wait()

	res, err := dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if res.(int) != -100 {
		t.Fatalf("Expected 100. Got: %v", res)
	}

}
