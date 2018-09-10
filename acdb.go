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
	Del(k string)
}

func NewMemDriver() *MemDriver {
	return &MemDriver{
		data: map[string][]byte{},
	}
}

type MemDriver struct {
	data map[string][]byte
}

func (d *MemDriver) Get(k string) ([]byte, error) {
	buf, b := d.data[k]
	if !b {
		return buf, errors.New("key error: " + k)
	}
	return buf, nil
}

func (d *MemDriver) Set(k string, v []byte) error {
	d.data[k] = v
	return nil
}

func (d *MemDriver) Del(k string) {
	delete(d.data, k)
}

func NewDocDriver(root string) *DocDriver {
	if err := os.MkdirAll(root, 0755); err != nil {
		panic(err)
	}
	return &DocDriver{
		root: root,
	}
}

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
	f, err := os.OpenFile(path.Join(d.root, k), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(v); err != nil {
		return err
	}
	return nil
}

func (d *DocDriver) Del(k string) {
	os.Remove(path.Join(d.root, k))
}

func NewLruDriver(size int) *LruDriver {
	return &LruDriver{
		driver: NewMemDriver(),
		m:      map[string]*list.Element{},
		l:      &list.List{},
		size:   size,
	}
}

type LruDriver struct {
	driver Driver
	m      map[string]*list.Element
	l      *list.List
	size   int
}

func (d *LruDriver) Get(k string) ([]byte, error) {
	buf, err := d.driver.Get(k)
	if err != nil {
		return []byte{}, err
	}
	d.l.MoveToFront(d.m[k])
	return buf, nil
}

func (d *LruDriver) Set(k string, v []byte) error {
	if d.l.Len() >= d.size {
		for i := 0; i < d.size/4; i++ {
			e := d.l.Back()
			k := e.Value.(string)
			d.Del(k)
		}
	}

	d.Del(k)
	if err := d.driver.Set(k, v); err != nil {
		return err
	}
	e := d.l.PushFront(k)
	d.m[k] = e
	return nil
}

func (d *LruDriver) Del(k string) {
	e, exist := d.m[k]
	if exist {
		d.driver.Del(k)
		d.l.Remove(e)
		delete(d.m, k)
	}
}

func NewMapDriver(root string) *MapDriver {
	return &MapDriver{
		doc: NewDocDriver(root),
		lru: NewLruDriver(1024),
	}
}

type MapDriver struct {
	doc *DocDriver
	lru *LruDriver
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

func (d *MapDriver) Del(k string) {
	d.doc.Del(k)
	d.lru.Del(k)
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

func (e *Emerge) Del(k string) {
	e.m.Lock()
	defer e.m.Unlock()
	e.driver.Del(k)
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

func Mem() *Emerge            { return NewEmerge(NewMemDriver()) }
func Doc(root string) *Emerge { return NewEmerge(NewDocDriver(root)) }
func Lru(size int) *Emerge    { return NewEmerge(NewLruDriver(size)) }
func Map(root string) *Emerge { return NewEmerge(NewMapDriver(root)) }
