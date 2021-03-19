package fsdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"syscall"

	"gopkg.in/yaml.v3"
)

func New(dataDir string) *DB {
	return &DB{
		dataDir: path.Clean(dataDir),
		lock:    &sync.RWMutex{},
	}
}

type DB struct {
	dataDir string
	lock    *sync.RWMutex
}

func (db *DB) Read(out interface{}, key ...string) (bool, error) {
	loc := db.filepath(key...)
	db.lock.RLock()
	bs, err := ioutil.ReadFile(loc)
	db.lock.RUnlock()

	if pErr, ok := err.(*os.PathError); ok && pErr.Err == syscall.ENOENT {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if err := yaml.Unmarshal(bs, out); err != nil {
		return true, err
	}
	return true, nil
}

func (db *DB) Write(in interface{}, key ...string) error {
	loc := db.filepath(key...)
	dir := path.Dir(loc)

	bs, err := yaml.Marshal(in)
	if err != nil {
		return err
	}

	db.lock.Lock()
	defer db.lock.Unlock()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return ioutil.WriteFile(loc, bs, 0644)
}

func (db *DB) Delete(key ...string) error {
	loc := db.filepath(key...)
	db.lock.Lock()
	defer db.lock.Unlock()
	return os.RemoveAll(loc)
}

func (db *DB) filepath(key ...string) string {
	key[len(key)-1] = fmt.Sprintf("%s.yaml", key[len(key)-1])
	elem := append([]string{db.dataDir}, key...)
	return path.Join(elem...)
}
