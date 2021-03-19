# fsdb

A very simple database backed by the filesystem.

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
