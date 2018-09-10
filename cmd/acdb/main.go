package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/mohanson/acdb"
)

var (
	flDriver = flag.String("driver", "lru", "Driver")
	flDoc    = flag.String("doc", "", "Doc root")
	flCap    = flag.Int("cap", 8192, "LRU cache size")
	flListen = flag.String("l", ":8080", "Listen address")
	flTLS    = flag.String("tls", "/etc/tls", "Path of TLS pems")
	emerge   acdb.Emerge
)

func hand(w http.ResponseWriter, r *http.Request) {
	clireq := &acdb.HTTPEmergeReq{}
	if err := json.NewDecoder(r.Body).Decode(clireq); err != nil {
		return
	}
	clires := &acdb.HTTPEmergeRes{K: clireq.K}
	defer json.NewEncoder(w).Encode(clires)

	switch strings.ToUpper(clireq.Command) {
	case "GET":
		buf, err := emerge.GetBytes(clireq.K)
		if err != nil {
			clires.Err = err.Error()
			return
		}
		clires.V = buf
		return
	case "SET":
		err := emerge.SetBytes(clireq.K, clireq.V)
		if err != nil {
			clires.Err = err.Error()
			return
		}
	case "DEL":
		emerge.Del(clireq.K)
		return
	case "ADD":
		var n int64
		if err := json.Unmarshal(clireq.V, &n); err != nil {
			clires.Err = err.Error()
			return
		}
		if err := emerge.Add(clireq.K, n); err != nil {
			clires.Err = err.Error()
			return
		}
	case "DEC":
		var n int64
		if err := json.Unmarshal(clireq.V, &n); err != nil {
			clires.Err = err.Error()
			return
		}
		if err := emerge.Dec(clireq.K, n); err != nil {
			clires.Err = err.Error()
			return
		}
	}
}

func root() string {
	if *flDoc != "" {
		return *flDoc
	}
	return path.Join(os.TempDir(), "acdb")
}

func main() {
	flag.Parse()

	switch *flDriver {
	case "mem":
		emerge = acdb.Mem()
	case "doc":
		emerge = acdb.Doc(root())
	case "lru":
		emerge = acdb.Lru(*flCap)
	case "map":
		emerge = acdb.Map(root(), *flCap)
	}

	var (
		caCrt     = path.Join(*flTLS, "ca.crt")
		serverCrt = path.Join(*flTLS, "server.crt")
		serverKey = path.Join(*flTLS, "server.key")
	)
	caData, err := ioutil.ReadFile(caCrt)
	if err != nil {
		log.Fatalln(err)
	}
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caData)
	server := &http.Server{
		Addr:    *flListen,
		Handler: http.HandlerFunc(hand),
		TLSConfig: &tls.Config{
			ClientAuth: tls.RequireAndVerifyClientCert,
			ClientCAs:  caPool,
		},
	}

	log.Println("Listen and serve TLS on", *flListen)
	if err := server.ListenAndServeTLS(serverCrt, serverKey); err != nil {
		log.Fatalln(err)
	}
}
