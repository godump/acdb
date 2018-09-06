package acdb

import (
	"bytes"
	"container/list"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Driver interface {
	Set(k string, v []byte) error
	Get(k string) ([]byte, error)
	Del(k string)
}

type MemDriver struct {
	memcache map[string][]byte
}

func (d *MemDriver) Get(k string) ([]byte, error) {
	buf, b := d.memcache[k]
	if !b {
		return buf, errors.New("key error: " + k)
	}
	return buf, nil
}

func (d *MemDriver) Set(k string, v []byte) error {
	d.memcache[k] = v
	return nil
}

func (d *MemDriver) Del(k string) {
	delete(d.memcache, k)
}

func NewMemDriver() *MemDriver {
	return &MemDriver{
		memcache: map[string][]byte{},
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
	f, err := os.OpenFile(path.Join(d.root, k), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
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

func NewDocDriver(root string) *DocDriver {
	if err := os.MkdirAll(root, 0755); err != nil {
		panic(err)
	}
	return &DocDriver{
		root: root,
	}
}

type LruDriver struct {
	driver Driver
	m      map[string]*list.Element
	l      *list.List
	cap    int
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
	if d.l.Len() >= d.cap {
		for i := 0; i < d.cap/4; i++ {
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

func NewLruDriver(cap int) *LruDriver {
	return &LruDriver{
		driver: NewMemDriver(),
		m:      map[string]*list.Element{},
		l:      &list.List{},
		cap:    cap,
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

func NewMapDriver(root string) *MapDriver {
	return &MapDriver{
		doc: NewDocDriver(root),
		lru: NewLruDriver(1024),
	}
}

type Emerge interface {
	GetBytes(k string) ([]byte, error)
	SetBytes(k string, v []byte) error
	Get(k string, v interface{}) error
	Set(k string, v interface{}) error
	Del(k string)
	Add(k string, n int64) error
	Dec(k string, n int64) error
}

type JSONEmerge struct {
	driver Driver
	m      *sync.RWMutex
}

func (e *JSONEmerge) GetBytes(k string) ([]byte, error) {
	e.m.RLock()
	defer e.m.RUnlock()
	return e.driver.Get(k)
}

func (e *JSONEmerge) SetBytes(k string, v []byte) error {
	e.m.Lock()
	defer e.m.Unlock()
	return e.driver.Set(k, v)
}

func (e *JSONEmerge) Get(k string, v interface{}) error {
	buf, err := e.GetBytes(k)
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, v)
}

func (e *JSONEmerge) Set(k string, v interface{}) error {
	buf, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return e.SetBytes(k, buf)
}

func (e *JSONEmerge) Del(k string) {
	e.m.Lock()
	defer e.m.Unlock()
	e.driver.Del(k)
}

func (e *JSONEmerge) Add(k string, n int64) error {
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

func (e *JSONEmerge) Dec(k string, n int64) error {
	return e.Add(k, -n)
}

func NewJSONEmerge(driver Driver) *JSONEmerge {
	return &JSONEmerge{driver: driver, m: &sync.RWMutex{}}
}

type HTTPEmerge struct {
	server string
	client *http.Client
}

type HTTPEmergeReq struct {
	Command string `json:"command"`
	K       string `json:"k"`
	V       []byte `json:"v"`
}

type HTTPEmergeRes struct {
	Err string `json:"err"`
	K   string `json:"k"`
	V   []byte `json:"v"`
}

func (e *HTTPEmerge) req(clireq *HTTPEmergeReq) (*HTTPEmergeRes, error) {
	clires := &HTTPEmergeRes{}
	data, err := json.Marshal(clireq)
	if err != nil {
		return clires, err
	}
	req, err := http.NewRequest("PUT", e.server, bytes.NewReader(data))
	if err != nil {
		return clires, err
	}
	res, err := e.client.Do(req)
	if err != nil {
		return clires, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return clires, errors.New(strconv.Itoa(res.StatusCode))
	}
	if err := json.NewDecoder(res.Body).Decode(clires); err != nil {
		return clires, err
	}
	if clires.Err != "" {
		return clires, errors.New(clires.Err)
	}
	return clires, nil
}

func (e *HTTPEmerge) GetBytes(k string) ([]byte, error) {
	clireq := &HTTPEmergeReq{Command: "GET", K: k}
	clires, err := e.req(clireq)
	return clires.V, err
}

func (e *HTTPEmerge) SetBytes(k string, v []byte) error {
	clireq := &HTTPEmergeReq{Command: "SET", K: k, V: v}
	_, err := e.req(clireq)
	return err
}

func (e *HTTPEmerge) Get(k string, v interface{}) error {
	buf, err := e.GetBytes(k)
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, v)
}

func (e *HTTPEmerge) Set(k string, v interface{}) error {
	buf, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return e.SetBytes(k, buf)
}

func (e *HTTPEmerge) Del(k string) {
	clireq := &HTTPEmergeReq{Command: "DEL", K: k}
	e.req(clireq)
}

func (e *HTTPEmerge) Add(k string, n int64) error {
	buf, _ := json.Marshal(n)
	clireq := &HTTPEmergeReq{Command: "ADD", K: k, V: buf}
	_, err := e.req(clireq)
	return err
}

func (e *HTTPEmerge) Dec(k string, n int64) error {
	buf, _ := json.Marshal(n)
	clireq := &HTTPEmergeReq{Command: "DEC", K: k, V: buf}
	_, err := e.req(clireq)
	return err
}

func NewHTTPEmerge(server string, conf string) *HTTPEmerge {
	var (
		caCrt     = path.Join(conf, "ca.crt")
		clientCrt = path.Join(conf, "client.crt")
		clientKey = path.Join(conf, "client.key")
	)
	caData, err := ioutil.ReadFile(caCrt)
	if err != nil {
		log.Fatalln(err)
	}
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caData)

	cliCrt, err := tls.LoadX509KeyPair(clientCrt, clientKey)
	if err != nil {
		log.Fatalln(err)
	}
	client := &http.Client{Transport: &http.Transport{
		Dial: func(network, addr string) (c net.Conn, err error) {
			c, err = net.DialTimeout(network, addr, 20*time.Second)
			if err != nil {
				return nil, err
			}
			return
		},
		TLSClientConfig: &tls.Config{
			RootCAs:      caPool,
			Certificates: []tls.Certificate{cliCrt},
		},
	}}

	if !strings.HasPrefix(server, "https://") {
		server = "https://" + server
	}
	return &HTTPEmerge{
		server: server,
		client: client,
	}
}

func Mem() Emerge                           { return NewJSONEmerge(NewMemDriver()) }
func Doc(root string) Emerge                { return NewJSONEmerge(NewDocDriver(root)) }
func Lru(cap int) Emerge                    { return NewJSONEmerge(NewLruDriver(cap)) }
func Map(root string) Emerge                { return NewJSONEmerge(NewMapDriver(root)) }
func Cli(server string, conf string) Emerge { return NewHTTPEmerge(server, conf) }
