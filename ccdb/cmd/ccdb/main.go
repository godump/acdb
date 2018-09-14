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
	flDriver = flag.String("driver", "lru", "Driver")
	flDoc    = flag.String("doc", path.Join(os.TempDir(), "acdb"), "Doc root")
	flCap    = flag.Int("cap", 1024, "LRU cache size")
	flListen = flag.String("l", ":8080", "Listen address")
)

var (
	client acdb.Client
)

func hand(w http.ResponseWriter, r *http.Request) {
	option := &ccdb.Option{}
	if err := json.NewDecoder(r.Body).Decode(option); err != nil {
		return
	}
	output := &ccdb.Output{K: option.K}
	defer json.NewEncoder(w).Encode(output)

	switch strings.ToUpper(option.Command) {
	case "GET":
		var raw json.RawMessage
		if err := client.Get(option.K, &raw); err != nil {
			output.Err = err.Error()
			return
		}
		output.V = raw
		return
	case "SET":
		if err := client.Set(option.K, json.RawMessage(option.V)); err != nil {
			output.Err = err.Error()
			return
		}
		output.V = option.V
		return
	case "DEL":
		client.Del(option.K)
		return
	case "ADD":
		var n int64
		if err := json.Unmarshal(option.V, &n); err != nil {
			output.Err = err.Error()
			return
		}
		if err := client.Add(option.K, n); err != nil {
			output.Err = err.Error()
			return
		}
	case "DEC":
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
}

func main() {
	flag.Parse()
	switch *flDriver {
	case "mem":
		client = acdb.Mem()
	case "doc":
		client = acdb.Doc(*flDoc)
	case "lru":
		client = acdb.LRU(*flCap)
	case "map":
		client = acdb.Map(*flDoc)
	}
	log.Println("Listen and serve on", *flListen)
	if err := http.ListenAndServe(*flListen, http.HandlerFunc(hand)); err != nil {
		log.Fatalln(err)
	}
}
