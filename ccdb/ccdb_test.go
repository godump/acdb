package ccdb

import (
	"sync"
	"testing"
)

func TestEmerge(t *testing.T) {
	e := Cli("http://127.0.0.1:8080")
	func() {
		if err := e.Set("name", "acdb"); err != nil {
			t.FailNow()
		}
		var r string
		if err := e.Get("name", &r); err != nil {
			t.FailNow()
		}
		if r != "acdb" {
			t.FailNow()
		}
		e.Del("name")
	}()

	func() {
		e.Set("n", 0)
		g := sync.WaitGroup{}
		g.Add(64)
		for i := 0; i < 64; i++ {
			go func() {
				defer g.Done()
				e.Add("n", 1)
			}()
		}
		g.Wait()
		var r int64
		e.Get("n", &r)
		if r != 64 {
			t.FailNow()
		}
	}()
}
