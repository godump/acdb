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

type Driver interface {
	Set(k string, v []byte) error
	Get(k string) ([]byte, error)
	Del(k string) error
}

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

func (d *MemDriver) Get(k string) ([]byte, error) {
	buf, b := d.data[k]
	if !b {
		return buf, errors.New("acdb: key error")
	}
	return buf, nil
}

func (d *MemDriver) Set(k string, v []byte) error {
	d.data[k] = v
	return nil
}

func (d *MemDriver) Del(k string) error {
	delete(d.data, k)
	return nil
}

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

func (d *DocDriver) Get(k string) ([]byte, error) {
	f, err := os.Open(path.Join(d.root, k))
	if err != nil {
		return []byte{}, err
	}
	defer f.Close()

	return ioutil.ReadAll(f)
}

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

func (d *DocDriver) Del(k string) error {
	return os.Remove(path.Join(d.root, k))
}

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

func (d *LRUDriver) Get(k string) ([]byte, error) {
	buf, err := d.driver.Get(k)
	if err != nil {
		return []byte{}, err
	}
	d.l.MoveToFront(d.m[k])
	return buf, nil
}

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

func (d *MapDriver) Get(k string) ([]byte, error) {
	var (
		buf []byte
		err error
	)
	buf, err = d.lru.Get(k)
	if err == nil {
		return buf, err
	}
	buf, err = d.doc.Get(k)
	if err != nil {
		return buf, err
	}
	err = d.lru.Set(k, buf)
	return buf, err
}

func (d *MapDriver) Set(k string, v []byte) error {
	if err := d.doc.Set(k, v); err != nil {
		return err
	}
	if err := d.lru.Set(k, v); err != nil {
		return err
	}
	return nil
}

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
	Get(string, interface{}) error
	Set(string, interface{}) error
	Del(string) error
	Add(string, int64) error
	Dec(string, int64) error
}

func NewEmerge(driver Driver) *Emerge {
	return &Emerge{driver: driver, m: &sync.Mutex{}}
}

type Emerge struct {
	driver Driver
	m      *sync.Mutex
}

func (e *Emerge) Get(k string, v interface{}) error {
	e.m.Lock()
	defer e.m.Unlock()
	buf, err := e.driver.Get(k)
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, v)
}

func (e *Emerge) Set(k string, v interface{}) error {
	e.m.Lock()
	defer e.m.Unlock()
	buf, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return e.driver.Set(k, buf)
}

func (e *Emerge) Del(k string) error {
	e.m.Lock()
	defer e.m.Unlock()
	return e.driver.Del(k)
}

func (e *Emerge) Add(k string, n int64) error {
	e.m.Lock()
	defer e.m.Unlock()
	var (
		i   int64
		buf []byte
		err error
	)
	buf, err = e.driver.Get(k)
	if err != nil {
		return err
	}
	err = json.Unmarshal(buf, &i)
	if err != nil {
		return err
	}
	i += n
	buf, err = json.Marshal(i)
	if err != nil {
		return err
	}
	return e.driver.Set(k, buf)
}

func (e *Emerge) Dec(k string, n int64) error {
	return e.Add(k, -n)
}

func Mem() Client            { return NewEmerge(NewMemDriver()) }
func Doc(root string) Client { return NewEmerge(NewDocDriver(root)) }
func LRU(size int) Client    { return NewEmerge(NewLRUDriver(size)) }
func Map(root string) Client { return NewEmerge(NewMapDriver(root)) }
