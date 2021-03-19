package fsdb_test

import (
	"io/ioutil"
	"os"
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
	ok, err := db.Read(&doc, "foo", "bar")
	assert.NoError(t, err)
	assert.False(t, ok)

	// Write new:

	written := Doc{Field: "value"}
	assert.NoError(t, db.Write(written, "bar", "baz"))

	// Read existing:

	var read Doc
	ok, err = db.Read(&read, "bar", "baz")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, written, read)

	// Remove missing and existing:

	assert.NoError(t, db.Delete("bogus"))
	assert.NoError(t, db.Delete("bar", "baz"))

	var gone Doc
	ok, err = db.Read(&gone, "bar", "baz")
	assert.False(t, ok)
	assert.NoError(t, err)
}
