package fsdb_test

import (
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/complex64/fsdb"
)

type Doc struct {
	Field string
}

func TestDB(t *testing.T) {
	dir, err := ioutil.TempDir("", "fsdb")
	assert.NoError(t, err)
	defer func() { _ = os.RemoveAll(dir) }()
	db := fsdb.New(dir)

	// Read missing:

	var doc Doc
	ok, err := db.Read(db.Key("foo", "bar"), &doc)
	assert.NoError(t, err)
	assert.False(t, ok)

	// Write new:

	written := Doc{Field: "value"}
	assert.NoError(t, db.Write(db.Key("bar", "baz"), written))

	written2 := Doc{Field: "value2"}
	assert.NoError(t, db.Write(db.Key("bar", "baz2"), written2))

	// Read existing:

	var read Doc
	ok, err = db.Read(db.Key("bar", "baz"), &read)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, written, read)

	// Read all:

	var keys []fsdb.Key
	var values []Doc
	err = db.ReadAll(db.Key("bar"), &keys, &values)

	assert.NoError(t, err)
	assert.Len(t, keys, 2)
	assert.Len(t, values, 2)

	sort.Slice(keys, func(i, j int) bool { return keys[i].Document() < keys[j].Document() })
	sort.Slice(values, func(i, j int) bool { return values[i].Field < values[j].Field })

	assert.Equal(t, db.Key("bar", "baz"), keys[0])
	assert.Equal(t, db.Key("bar", "baz2"), keys[1])
	assert.Equal(t, written, values[0])
	assert.Equal(t, written2, values[1])

	// Remove missing and existing:

	assert.NoError(t, db.Delete(db.Key("bogus")))
	assert.NoError(t, db.Delete(db.Key("bar", "baz")))

	var gone Doc
	ok, err = db.Read(db.Key("bar", "baz"), &gone)
	assert.False(t, ok)
	assert.NoError(t, err)
}

func TestKey(t *testing.T) {
	db := fsdb.New("/tmp")
	key := db.Key("foo", "bar")
	assert.Equal(t, "/tmp/foo/bar", key.Prefix())
	assert.Equal(t, "/tmp/foo/bar"+fsdb.Extension, key.Document())
}
