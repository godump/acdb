// Copyright 2018 Mohanson. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

package acdb

import (
	"container/list"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

// Portable analogs of some common system call errors.
var (
	ErrNotExist = errors.New("acdb: key does not exist")
)

// Driver is the interface that wraps the Set/Get and Del method.
//
// Set sets bytes with given k.
// Get gets and returns the bytes or any error encountered. If the key does
// not exist, ErrNotExist will be returned.
// Del dels bytes with given k. If the key does not exist, ErrNotExist will
// be returned.
type Driver interface {
	Set(k string, v []byte) error
	Get(k string) ([]byte, error)
	Del(k string) error
}

// NewMemDriver returns a MemDriver.
func NewMemDriver() *MemDriver {
	return &MemDriver{
		data: map[string][]byte{},
	}
}

// MemDriver cares to store data on memory, this means that MemDriver is fast.
// Since there is no expiration mechanism, be careful that it might eats up all
// your memory.
type MemDriver struct {
	data map[string][]byte
}

// Get gets and returns the bytes or any error encountered. If the key does
// not exist, ErrNotExist will be returned.
func (d *MemDriver) Get(k string) ([]byte, error) {
	buf, b := d.data[k]
	if !b {
		return buf, ErrNotExist
	}
	return buf, nil
}

// Set sets bytes with given k.
func (d *MemDriver) Set(k string, v []byte) error {
	d.data[k] = v
	return nil
}

// Del dels bytes with given k. If the key does not exist, ErrNotExist will
// be returned.
func (d *MemDriver) Del(k string) error {
	delete(d.data, k)
	return nil
}

// NewDocDriver returns a DocDriver.
func NewDocDriver(root string) *DocDriver {
	if err := os.MkdirAll(root, 0755); err != nil {
		panic(err)
	}
	return &DocDriver{
		root: root,
	}
}

// DocDriver use the OS's file system to manage data. In general, any high
// frequency operation is not recommended unless you have an enough reason.
type DocDriver struct {
	root string
}

// Get gets and returns the bytes or any error encountered. If the key does
// not exist, ErrNotExist will be returned.
func (d *DocDriver) Get(k string) ([]byte, error) {
	f, err := os.Open(path.Join(d.root, k))
	if err != nil {
		if os.IsNotExist(err) {
			return []byte{}, ErrNotExist
		}
		return []byte{}, err
	}
	defer f.Close()

	return ioutil.ReadAll(f)
}

// Set sets bytes with given k.
func (d *DocDriver) Set(k string, v []byte) error {
	f, err := os.OpenFile(path.Join(d.root, k),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(v); err != nil {
		return err
	}
	return nil
}

// Del dels bytes with given k. If the key does not exist, ErrNotExist will
// be returned.
func (d *DocDriver) Del(k string) error {
	err := os.Remove(path.Join(d.root, k))
	if os.IsNotExist(err) {
		return ErrNotExist
	}
	return err
}

// NewLRUDriver returns a LRUDriver.
func NewLRUDriver(size int) *LRUDriver {
	return &LRUDriver{
		driver: NewMemDriver(),
		m:      map[string]*list.Element{},
		l:      &list.List{},
		size:   size,
	}
}

// In computing, cache algorithms (also frequently called cache replacement
// algorithms or cache replacement policies) are optimizing instructions, or
// algorithms, that a computer program or a hardware-maintained structure can
// utilize in order to manage a cache of information stored on the computer.
// Caching improves performance by keeping recent or often-used data items in
// a memory locations that are faster or computationally cheaper to access than
// normal memory stores. When the cache is full, the algorithm must choose
// which items to discard to make room for the new ones.
//
// Least recently used (LRU), discards the least recently used items first. It
// has a fixed size(for limit memory usages) and O(1) time lookup.
type LRUDriver struct {
	driver Driver
	m      map[string]*list.Element
	l      *list.List
	size   int
}

// Get gets and returns the bytes or any error encountered. If the key does
// not exist, ErrNotExist will be returned.
func (d *LRUDriver) Get(k string) ([]byte, error) {
	buf, err := d.driver.Get(k)
	if err != nil {
		return []byte{}, err
	}
	d.l.MoveToFront(d.m[k])
	return buf, nil
}

// Set sets bytes with given k.
func (d *LRUDriver) Set(k string, v []byte) error {
	if d.l.Len() >= d.size {
		for i := 0; i < d.size/4; i++ {
			e := d.l.Back()
			k := e.Value.(string)
			if err := d.Del(k); err != nil {
				return err
			}
		}
	}

	if err := d.Del(k); err != nil {
		return err
	}
	if err := d.driver.Set(k, v); err != nil {
		return err
	}
	e := d.l.PushFront(k)
	d.m[k] = e
	return nil
}

// Del dels bytes with given k. If the key does not exist, ErrNotExist will
// be returned.
func (d *LRUDriver) Del(k string) error {
	e, exist := d.m[k]
	if exist {
		if err := d.driver.Del(k); err != nil {
			return err
		}
		d.l.Remove(e)
		delete(d.m, k)
	}
	return nil
}

// NewMapDriver returns a MapDriver.
func NewMapDriver(root string) *MapDriver {
	return &MapDriver{
		doc: NewDocDriver(root),
		lru: NewLRUDriver(1024),
	}
}

// MapDriver is based on DocDriver and use LRUDriver to provide caching at its
// interface layer. The size of LRUDriver is always 1024.
type MapDriver struct {
	doc *DocDriver
	lru *LRUDriver
}

// Get gets and returns the bytes or any error encountered. If the key does
// not exist, ErrNotExist will be returned.
func (d *MapDriver) Get(k string) ([]byte, error) {
	var (
		buf []byte
		err error
	)
	buf, err = d.lru.Get(k)
	if err == nil {
		return buf, nil
	}
	buf, err = d.doc.Get(k)
	if err != nil {
		return buf, err
	}
	err = d.lru.Set(k, buf)
	return buf, err
}

// Set sets bytes with given k.
func (d *MapDriver) Set(k string, v []byte) error {
	if err := d.doc.Set(k, v); err != nil {
		return err
	}
	if err := d.lru.Set(k, v); err != nil {
		return err
	}
	return nil
}

// Del dels bytes with given k. If the key does not exist, ErrNotExist will
// be returned.
func (d *MapDriver) Del(k string) error {
	if err := d.doc.Del(k); err != nil {
		return err
	}
	if err := d.lru.Del(k); err != nil {
		return err
	}
	return nil
}

type Client interface {
	// Get gets and returns the bytes or any error encountered. If the key does
	// not exist, ErrNotExist will be returned.
	Get(string, interface{}) error
	// Set sets bytes with given k.
	Set(string, interface{}) error
	// Del dels bytes with given k. If the key does not exist, ErrNotExist will
	// be returned.
	Del(string) error
	// Add increments the number stored at key by n.
	Add(string, int64) error
	// Dec decrements the number stored at key by n.
	Dec(string, int64) error
	// Set key to hold value if key does not exist. In that case, it is equal
	// to Set. When key already holds a value, no operation is performed. SETNX
	// is short for "SET if Not exists".
	SetNx(string, interface{}) error
}

// NewEmerge returns a Emerge.
func NewEmerge(driver Driver) *Emerge {
	return &Emerge{driver: driver, m: &sync.Mutex{}}
}

// Emerge is a actuator of the given drive. Do not worry, Is's
// concurrency-safety.
type Emerge struct {
	driver Driver
	m      *sync.Mutex
}

func (e *Emerge) get(k string, v interface{}) error {
	buf, err := e.driver.Get(k)
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, v)
}

func (e *Emerge) set(k string, v interface{}) error {
	buf, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return e.driver.Set(k, buf)
}

// Get gets and returns the bytes or any error encountered. If the key does
// not exist, ErrNotExist will be returned.
func (e *Emerge) Get(k string, v interface{}) error {
	e.m.Lock()
	defer e.m.Unlock()
	return e.get(k, v)
}

// Set sets bytes with given k.
func (e *Emerge) Set(k string, v interface{}) error {
	e.m.Lock()
	defer e.m.Unlock()
	return e.set(k, v)
}

// Set key to hold value if key does not exist. In that case, it is equal to
// Set. When key already holds a value, no operation is performed. SETNX is
// short for "SET if Not exists".
func (e *Emerge) SetNx(k string, v interface{}) error {
	e.m.Lock()
	defer e.m.Unlock()
	var a json.RawMessage
	err := e.get(k, &a)
	if err == nil {
		return nil
	}
	if err == ErrNotExist {
		return e.set(k, v)
	}
	return err
}

// Del dels bytes with given k. If the key does not exist, ErrNotExist will
// be returned.
func (e *Emerge) Del(k string) error {
	e.m.Lock()
	defer e.m.Unlock()
	return e.driver.Del(k)
}

// Add increments the number stored at key by n.
func (e *Emerge) Add(k string, n int64) error {
	e.m.Lock()
	defer e.m.Unlock()
	var i int64
	if err := e.get(k, &i); err != nil {
		return err
	}
	return e.set(k, i+n)
}

// Dec decrements the number stored at key by n.
func (e *Emerge) Dec(k string, n int64) error {
	return e.Add(k, -n)
}

// Mem returns a concurrency-safety Client with MemDriver.
func Mem() Client { return NewEmerge(NewMemDriver()) }

// Doc returns a concurrency-safety Client with DocDriver.
func Doc(root string) Client { return NewEmerge(NewDocDriver(root)) }

// LRU returns a concurrency-safety Client with LRUDriver.
func LRU(size int) Client { return NewEmerge(NewLRUDriver(size)) }

// Map returns a concurrency-safety Client with MapDriver.
func Map(root string) Client { return NewEmerge(NewMapDriver(root)) }
