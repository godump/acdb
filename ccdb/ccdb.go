package ccdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/mohanson/acdb"
)

type Option struct {
	Command string `json:"command"`
	K       string `json:"k"`
	V       []byte `json:"v"`
}

type Output struct {
	Err string `json:"err"`
	K   string `json:"k"`
	V   []byte `json:"v"`
}

func NewEmerge(server string) *Emerge {
	return &Emerge{
		server: server,
		client: http.DefaultClient,
	}
}

type Emerge struct {
	server string
	client *http.Client
}

func (e *Emerge) Cmd(option *Option) (*Output, error) {
	output := &Output{}
	buf, err := json.Marshal(option)
	if err != nil {
		return output, err
	}
	req, err := http.NewRequest("PUT", e.server, bytes.NewReader(buf))
	if err != nil {
		return output, err
	}
	res, err := e.client.Do(req)
	if err != nil {
		return output, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return output, errors.New(strconv.Itoa(res.StatusCode))
	}
	if err := json.NewDecoder(res.Body).Decode(output); err != nil {
		return output, err
	}
	if output.Err != "" {
		return output, errors.New(output.Err)
	}
	return output, nil
}

func (e *Emerge) Get(k string, v interface{}) error {
	output, err := e.Cmd(&Option{Command: "GET", K: k})
	if err != nil {
		return err
	}
	return json.Unmarshal(output.V, v)
}

func (e *Emerge) Set(k string, v interface{}) error {
	var (
		b   []byte
		err error
	)
	b, err = json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = e.Cmd(&Option{Command: "SET", K: k, V: b})
	return err
}

func (e *Emerge) Del(k string) {
	option := &Option{Command: "DEL", K: k}
	e.Cmd(option)
}

func (e *Emerge) Add(k string, n int64) error {
	option := &Option{Command: "ADD", K: k, V: []byte(strconv.FormatInt(n, 10))}
	_, err := e.Cmd(option)
	return err
}

func (e *Emerge) Dec(k string, n int64) error {
	option := &Option{Command: "DEC", K: k, V: []byte(strconv.FormatInt(n, 10))}
	_, err := e.Cmd(option)
	return err
}

func Cli(server string) acdb.Client {
	return NewEmerge(server)
}
