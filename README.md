# Acdb

Package acdb manages objects between memory and file system.

Acdb is a highly available NoSQL data store that offloads the work of database administration. Developers simply set and get k/v data from memory and file system and Acdb does the rest.

The most common way of use Acdb is:

```go
package main

import (
	"github.com/godump/acdb"
)

func main() {
	db := acdb.Map("/tmp")
	db.SetEncode("price", 42)
	var u uint64
	db.GetDecode("price", &u)
}
```

Doc: [https://godoc.org/github.com/godump/acdb](https://godoc.org/github.com/godump/acdb)
