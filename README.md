Package **acdb** manages objects between memory and file system.

## Requirements

- acdb has been tested and is known to run on Linux/Ubuntu, macOS and Windows(10). It will likely work fine on most OS.
- [Go](http://golang.org) 1.8 or newer.

## Installation

```sh
go get github.com/mohanson/acdb
```

## Example

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
