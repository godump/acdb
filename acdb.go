package acdb

import (
	"encoding/json"
	"log"
	"os"
	"path"
	"sync"

	"github.com/godump/doa"
	"github.com/godump/lru"
)

// Driver is the interface that wraps the Set/Get and Del method.
//
// Get gets and returns the bytes or any error encountered. If the key does not exist, ErrNotExist will be returned.
// Set sets bytes with given k.
// Del dels bytes with given k. If the key does not exist, ErrNotExist will be returned.
type Driver interface {
	Get(k string) ([]byte, error)
	Set(k string, v []byte) error
	Del(k string) error
}

// MemDriver cares to store data on memory, this means that MemDriver is fast. Since there is no expiration mechanism,
// be careful that it might eats up all your memory.
type MemDriver struct {
	data map[string][]byte
}

// NewMemDriver returns a MemDriver.
func NewMemDriver() *MemDriver {
	return &MemDriver{
		data: map[string][]byte{},
	}
}

// Get the value of a key.
func (d *MemDriver) Get(k string) ([]byte, error) {
	v, b := d.data[k]
	if b {
		return v, nil
	}
	return nil, os.ErrNotExist
}

// Set the value of a key.
func (d *MemDriver) Set(k string, v []byte) error {
	d.data[k] = v
	return nil
}

// Del the value of a key.
func (d *MemDriver) Del(k string) error {
	delete(d.data, k)
	return nil
}

// DocDriver use the OS's file system to manage data. In general, any high frequency operation is not recommended
// unless you have an enough reason.
type DocDriver struct {
	root string
}

// NewDocDriver returns a DocDriver.
func NewDocDriver(root string) *DocDriver {
	doa.Nil(os.MkdirAll(root, 0755))
	return &DocDriver{
		root: root,
	}
}

// Get the value of a key.
func (d *DocDriver) Get(k string) ([]byte, error) {
	return os.ReadFile(path.Join(d.root, k))
}

// Set the value of a key.
func (d *DocDriver) Set(k string, v []byte) error {
	return os.WriteFile(path.Join(d.root, k), v, 0644)
}

// Del the value of a key.
func (d *DocDriver) Del(k string) error {
	return os.Remove(path.Join(d.root, k))
}

// LruDriver implemention. In computing, cache algorithms (also frequently called cache replacement algorithms or cache
// replacement policies) are optimizing instructions, or algorithms, that a computer program or a hardware-maintained
// structure can utilize in order to manage a cache of information stored on the computer. Caching improves performance
// by keeping recent or often-used data items in a memory locations that are faster or computationally cheaper to access
// than normal memory stores. When the cache is full, the algorithm must choose which items to discard to make room for
// the new ones.
//
// Least recently used (LRU), discards the least recently used items first. It has a fixed size(for limit memory usages)
// and O(1) time lookup.
type LruDriver struct {
	data *lru.Lru
}

// NewLruDriver returns a LruDriver.
func NewLruDriver(size int) *LruDriver {
	return &LruDriver{
		data: lru.NewLru(size),
	}
}

// Get the value of a key.
func (d *LruDriver) Get(k string) ([]byte, error) {
	v, b := d.data.Get(k)
	if b {
		return v.([]byte), nil
	}
	return nil, os.ErrNotExist
}

// Set the value of a key.
func (d *LruDriver) Set(k string, v []byte) error {
	d.data.Set(k, v)
	return nil
}

// Del the value of a key.
func (d *LruDriver) Del(k string) error {
	d.data.Del(k)
	return nil
}

// MapDriver is based on DocDriver and use LruDriver to provide caching at its
// interface layer. The size of LruDriver is always 1024.
type MapDriver struct {
	doc *DocDriver
	lru *LruDriver
}

// NewMapDriver returns a MapDriver.
func NewMapDriver(root string) *MapDriver {
	return &MapDriver{
		doc: NewDocDriver(root),
		lru: NewLruDriver(1024),
	}
}

// Get the value of a key.
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
		return nil, err
	}
	err = d.lru.Set(k, buf)
	return buf, err
}

// Set the value of a key.
func (d *MapDriver) Set(k string, v []byte) error {
	if err := d.lru.Set(k, v); err != nil {
		return err
	}
	if err := d.doc.Set(k, v); err != nil {
		return err
	}
	return nil
}

// Del the value of a key.
func (d *MapDriver) Del(k string) error {
	if err := d.lru.Del(k); err != nil {
		return err
	}
	if err := d.doc.Del(k); err != nil {
		return err
	}
	return nil
}

// Client is a actuator of the given drive. Do not worry, Is's concurrency-safety.
type Client struct {
	driver Driver
	m      *sync.Mutex
}

// NewClient returns a Client.
func NewClient(driver Driver) *Client {
	return &Client{driver: driver, m: &sync.Mutex{}}
}

// Get the value of a key.
func (e *Client) Get(k string) ([]byte, error) {
	e.m.Lock()
	defer e.m.Unlock()
	return e.driver.Get(k)
}

// Set the value of a key.
func (e *Client) Set(k string, v []byte) error {
	e.m.Lock()
	defer e.m.Unlock()
	log.Println("acdb: set", k, string(v))
	return e.driver.Set(k, v)
}

// GetDecode get the decoded value of a key.
func (e *Client) GetDecode(k string, v interface{}) error {
	b, err := e.Get(k)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// SetEncode set the encoded value of a key.
func (e *Client) SetEncode(k string, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return e.Set(k, b)
}

// GetUint32 get the uint32 value of a key.
func (e *Client) GetUint32(k string) (uint32, error) {
	var r uint32
	err := e.GetDecode(k, &r)
	return r, err
}

// GetUint64 get the uint64 value of a key.
func (e *Client) GetUint64(k string) (uint64, error) {
	var r uint64
	err := e.GetDecode(k, &r)
	return r, err
}

// GetFloat32 get the float32 value of a key.
func (e *Client) GetFloat32(k string) (float32, error) {
	var r float32
	err := e.GetDecode(k, &r)
	return r, err
}

// GetFloat64 get the float64 value of a key.
func (e *Client) GetFloat64(k string) (float64, error) {
	var r float64
	err := e.GetDecode(k, &r)
	return r, err
}

// GetString get the string value of a key.
func (e *Client) GetString(k string) (string, error) {
	var r string
	err := e.GetDecode(k, &r)
	return r, err
}

// Del the value of a key.
func (e *Client) Del(k string) error {
	e.m.Lock()
	defer e.m.Unlock()
	return e.driver.Del(k)
}

// Has determine if a key exists.
func (e *Client) Has(k string) bool {
	_, err := e.Get(k)
	return err == nil
}

// Nil determine if a key emptys.
func (e *Client) Nil(k string) bool {
	_, err := e.Get(k)
	return err != nil
}

// Mem returns a concurrency-safety Client with MemDriver.
func Mem() *Client { return NewClient(NewMemDriver()) }

// Doc returns a concurrency-safety Client with DocDriver.
func Doc(root string) *Client { return NewClient(NewDocDriver(root)) }

// Lru returns a concurrency-safety Client with LruDriver.
func Lru(size int) *Client { return NewClient(NewLruDriver(size)) }

// Map returns a concurrency-safety Client with MapDriver.
func Map(root string) *Client { return NewClient(NewMapDriver(root)) }
