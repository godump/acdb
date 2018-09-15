package ccdb

import (
	"os/exec"
	"sync"
	"testing"
	"time"
)

func TestEmerge(t *testing.T) {
	cmd := exec.Command("ccdb", "-secret", "2e26d49a33c04f8c4d3615d9614c0c07")
	cmd.Start()
	defer cmd.Process.Kill()
	time.Sleep(time.Second)

	e := Cli("http://127.0.0.1:8080", "2e26d49a33c04f8c4d3615d9614c0c07")
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
