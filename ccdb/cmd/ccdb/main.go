package main

import (
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
	flDriverMem = flag.Bool("driver-mem", false, "Use acdb.Mem() for driver")
	flDriverDoc = flag.Bool("driver-doc", false, "Use acdb.Doc() for driver")
	flDriverLRU = flag.Bool("driver-lru", false, "Use acdb.LRU() for driver")
	flDriverMap = flag.Bool("driver-map", false, "Use acdb.Map() for driver")
	flPath      = flag.String("path", path.Join(os.TempDir(), "acdb"), "Directory to store data")
	flSize      = flag.Int("size", 1024, "Database size")
	flListen    = flag.String("l", ":8080", "Listen address")
)

var (
	client acdb.Client
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
	option := &ccdb.Option{}
	if err := json.NewDecoder(r.Body).Decode(option); err != nil {
		return
	}
	output := &ccdb.Output{K: option.K}
	defer json.NewEncoder(w).Encode(output)

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

	log.Println("Listen and serve on", *flListen)
	if err := http.ListenAndServe(*flListen, http.HandlerFunc(serve)); err != nil {
		log.Fatalln(err)
	}
}
