package ccdb

import (
	"crypto/cipher"
	"crypto/md5"
	"crypto/rc4"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math/rand"
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

func NewEmerge(server string, secret string) *Emerge {
	h := md5.Sum([]byte(secret))
	return &Emerge{
		server: server,
		client: http.DefaultClient,
		secret: h[:],
	}
}

type Emerge struct {
	server string
	client *http.Client
	secret []byte
}

func (e *Emerge) Cmd(option *Option) (*Output, error) {
	pipeReader, pipeWriter := io.Pipe()
	suffix := make([]byte, 16)
	rand.Read(suffix)
	go func() {
		defer pipeWriter.Close()
		c, err := rc4.NewCipher(append(e.secret, suffix...))
		if err != nil {
			log.Fatalln(err)
		}
		w := cipher.StreamWriter{S: c, W: pipeWriter}
		if err := json.NewEncoder(w).Encode(option); err != nil {
			log.Fatalln(err)
		}
	}()

	output := &Output{}
	req, err := http.NewRequest("PUT", e.server, pipeReader)
	if err != nil {
		return output, err
	}
	req.Header.Set("Secret-Suffix", hex.EncodeToString(suffix))
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

func Cli(server string, secret string) acdb.Client {
	return NewEmerge(server, secret)
}
