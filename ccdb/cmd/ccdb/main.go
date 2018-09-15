package main

import (
	"crypto/cipher"
	"crypto/md5"
	"crypto/rc4"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/mohanson/acdb"
	"github.com/mohanson/acdb/ccdb"
)

var (
	flDriverMem = flag.Bool("mem", false, "Use acdb.Mem() for driver")
	flDriverDoc = flag.Bool("doc", false, "Use acdb.Doc() for driver")
	flDriverLRU = flag.Bool("lru", false, "Use acdb.LRU() for driver")
	flDriverMap = flag.Bool("map", false, "Use acdb.Map() for driver")
	flPath      = flag.String("path", path.Join(os.TempDir(), "acdb"), "Directory to store data")
	flSize      = flag.Int("size", 1024, "Database size")
	flListen    = flag.String("l", ":8080", "Listen address")
	flSecret    = flag.String("secret", "", "Secret")
)

var (
	client acdb.Client
	secret []byte
)

func serveGet(option *ccdb.Option, output *ccdb.Output) {
	var raw json.RawMessage
	if err := client.Get(option.K, &raw); err != nil {
		output.Err = err.Error()
		return
	}
	output.V = raw
}

func serveSet(option *ccdb.Option, output *ccdb.Output) {
	if err := client.Set(option.K, json.RawMessage(option.V)); err != nil {
		output.Err = err.Error()
	}
}

func serveDel(option *ccdb.Option, output *ccdb.Output) {
	client.Del(option.K)
}

func serveAdd(option *ccdb.Option, output *ccdb.Output) {
	var n int64
	if err := json.Unmarshal(option.V, &n); err != nil {
		output.Err = err.Error()
		return
	}
	if err := client.Add(option.K, n); err != nil {
		output.Err = err.Error()
		return
	}
}

func serveDec(option *ccdb.Option, output *ccdb.Output) {
	var n int64
	if err := json.Unmarshal(option.V, &n); err != nil {
		output.Err = err.Error()
		return
	}
	if err := client.Dec(option.K, n); err != nil {
		output.Err = err.Error()
		return
	}
}

func serve(w http.ResponseWriter, r *http.Request) {
	c, _ := rc4.NewCipher(secret)
	reader := cipher.StreamReader{S: c, R: r.Body}
	option := &ccdb.Option{}
	if err := json.NewDecoder(reader).Decode(option); err != nil {
		return
	}
	output := &ccdb.Output{K: option.K}
	defer json.NewEncoder(w).Encode(output)

	log.Println(option.Command, option.K, string(option.V))
	switch strings.ToUpper(option.Command) {
	case "GET":
		serveGet(option, output)
	case "SET":
		serveSet(option, output)
	case "DEL":
		serveDel(option, output)
	case "ADD":
		serveAdd(option, output)
	case "DEC":
		serveDec(option, output)
	}
}

func main() {
	flag.Parse()

	client = func() acdb.Client {
		if *flDriverMem {
			return acdb.Mem()
		} else if *flDriverDoc {
			return acdb.Doc(*flPath)
		} else if *flDriverLRU {
			return acdb.LRU(*flSize)
		} else if *flDriverMap {
			return acdb.Map(*flPath)
		} else {
			return acdb.LRU(*flSize)
		}
	}()

	secret = func() []byte {
		h := md5.Sum([]byte(*flSecret))
		return h[:]
	}()

	log.Println("Listen and serve on", *flListen)
	if err := http.ListenAndServe(*flListen, http.HandlerFunc(serve)); err != nil {
		log.Fatalln(err)
	}
}
