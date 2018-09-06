Package **acdb** manages objects between memory and file system.

**acdb** is dead simple to use:

```sh
go get -u -v github.com/mohanson/acdb
```

```go
package main

import (
	"github.com/mohanson/acdb"
)

func main() {
	ss := acdb.Doc("/tmp/acdb")
	ss.Set("name", "acdb") // save an object
	var r string           // load an object
	ss.Get("name", &r)
	println(r)
}
```
