package acdb

import (
	"bytes"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"
)

func driverEasyCase(t *testing.T, d Driver) {
	v := []byte("acdb")
	if err := d.Set("name", v); err != nil {
		t.FailNow()
	}
	buf, err := d.Get("name")
	if err != nil {
		t.FailNow()
	}
	if !bytes.Equal(buf, v) {
		t.FailNow()
	}
	d.Del("name")
	if _, err := d.Get("name"); err != ErrNotExist {
		t.FailNow()
	}
}

func TestMemDriver(t *testing.T) {
	d := NewMemDriver()
	driverEasyCase(t, d)
}

func TestDocDriver(t *testing.T) {
	d := NewDocDriver(path.Join(os.TempDir(), "acdb"))
	driverEasyCase(t, d)
}

func TestLRUDriver(t *testing.T) {
	d := NewLRUDriver(1024)
	driverEasyCase(t, d)
}

func TestLRUDriverFull(t *testing.T) {
	d := NewLRUDriver(1024)
	if d.l.Len() != 0 || len(d.m) != 0 {
		t.FailNow()
	}
	for i := 0; i < 1024; i++ {
		istr := strconv.Itoa(i)
		d.Set(istr, []byte(istr))
	}
	if d.l.Len() != 1024 || len(d.m) != 1024 {
		t.FailNow()
	}
	if d.l.Front().Value.(string) != "1023" {
		t.FailNow()
	}
	d.Set("1024", []byte("1024"))
	if d.l.Len() != 769 || len(d.m) != 769 {
		t.FailNow()
	}
	if d.l.Front().Value.(string) != "1024" {
		t.FailNow()
	}
	d.Get("512")
	if d.l.Front().Value.(string) != "512" {
		t.FailNow()
	}
	if _, err := d.Get("0"); err == nil {
		t.FailNow()
	}
}

func TestMapDriver(t *testing.T) {
	d := NewMapDriver(path.Join(os.TempDir(), "acdb"))
	driverEasyCase(t, d)
}

func TestEmerge(t *testing.T) {
	e := Mem()

	// Test Get/Set/Del
	func() {
		defer e.Del("k")
		if err := e.Set("k", "v"); err != nil {
			t.FailNow()
		}
		var r string
		if err := e.Get("k", &r); err != nil {
			t.FailNow()
		}
		if r != "v" {
			t.FailNow()
		}
		e.Del("k")
	}()

	// Test Add/Dec
	func() {
		defer e.Del("n")
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

	// Test Some/None
	func() {
		defer e.Del("k")
		if !e.None("k") {
			t.FailNow()
		}
		if err := e.Set("k", "v"); err != nil {
			t.FailNow()
		}
		if !e.Some("k") {
			t.FailNow()
		}
	}()

	// Test SetSome/SetNone
	func() {
		defer e.Del("k1")
		defer e.Del("k2")
		var r string
		if err := e.SetNone("k1", "v1"); err != nil {
			t.FailNow()
		}
		e.Get("k1", &r)
		if r != "v1" {
			t.FailNow()
		}
		if err := e.SetNone("k1", "v2"); err != ErrHasExist {
			t.FailNow()
		}
		e.Get("k1", &r)
		if r != "v1" {
			t.FailNow()
		}

		if err := e.SetSome("k2", "v1"); err != ErrNotExist {
			t.FailNow()
		}
		e.Set("k2", "v1")
		if err := e.SetSome("k2", "v2"); err != nil {
			t.FailNow()
		}
		e.Get("k2", &r)
		if r != "v2" {
			t.FailNow()
		}
	}()
}
