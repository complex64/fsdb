# fsdb

[![Continuous Integration](https://github.com/complex64/fsdb/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/complex64/fsdb/actions/workflows/ci.yml)

A very simple database backed by the file system.

## Usage

```go
package main

import (
	"github.com/complex64/fsdb"
)

type Document struct{ Field string }

func main() {
	doc := Document{Field: "value"}
	
	db := fsdb.New("mydatadir")
	if err := db.Write(doc, "my", "key"); err != nil {
		panic(err)
	}
	// db.Read, db.Delete, ...
}
```
