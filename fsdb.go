package fsdb

import (
	"context"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"gopkg.in/yaml.v3"
)

const (
	Extension = ".yaml"
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

func (db *DB) Read(key Key, out interface{}) (bool, error) {
	db.lock.RLock()
	bs, err := ioutil.ReadFile(key.Document())
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

func (db *DB) Write(key Key, in interface{}) error {
	bs, err := yaml.Marshal(in)
	if err != nil {
		return err
	}

	db.lock.Lock()
	defer db.lock.Unlock()
	if err := os.MkdirAll(key.Prefix(), 0755); err != nil {
		return err
	}
	return ioutil.WriteFile(key.Document(), bs, 0644)
}

func (db *DB) Delete(key Key) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	return os.RemoveAll(key.Document())
}

func (db *DB) ReadAll(key Key, keys *[]Key, values interface{}) error {
	db.lock.RLock()
	entries, err := db.readAll(key)
	db.lock.RUnlock()

	if err != nil {
		return err
	}

	sliceType := reflect.ValueOf(values).Elem().Type()
	slice := reflect.MakeSlice(sliceType, 0, len(entries))
	ks := make([]Key, 0, len(entries))

	fmt.Println(slice)

	var (
		routines = runtime.NumCPU()
		inputs   = make(chan encoded, routines)
		outputs  = make(chan decoded, routines)
	)

	go func() {
		defer close(inputs)
		for _, e := range entries {
			inputs <- e
		}
	}()

	done := make(chan interface{})
	go func() {
		for e := range outputs {
			ks = append(ks, e.key)
			slice = reflect.Append(slice, reflect.ValueOf(e.value))
		}
		done <- struct{}{}
	}()

	valueType := sliceType.Elem()
	ctx, errs := db.decodeAsync(routines, inputs, outputs, valueType)
	<-ctx.Done()

	if err, ok := <-errs; !ok && err != nil {
		return err
	}

	<-done
	*keys = ks
	reflect.ValueOf(values).Elem().Set(slice)
	return nil
}

func (db *DB) decodeAsync(
	routines int,
	inputs <-chan encoded,
	outputs chan<- decoded,
	valueType reflect.Type,
) (
	context.Context,
	<-chan error,
) {
	errs := make(chan error, routines)
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(routines)

	go func() {
		wg.Wait()
		close(errs)
		close(outputs)
		cancel()
	}()

	for i := 0; i < routines; i++ {
		go func() {
			defer wg.Done()
			for entry := range inputs {
				zero := reflect.Zero(valueType)
				ptr := reflect.New(valueType)
				ptr.Elem().Set(zero)

				err := yaml.Unmarshal(entry.bytes, ptr.Interface())
				if err != nil {
					errs <- err
					return
				}

				outputs <- decoded{
					key:   db.parseKey(entry.path),
					value: ptr.Elem().Interface(),
				}
			}
		}()
	}

	return ctx, errs
}

func (db *DB) readAll(key Key) ([]encoded, error) {
	var (
		bufSize  = 256
		routines = runtime.NumCPU()

		inputs  = make(chan string, bufSize)
		outputs = make(chan encoded, routines)
		entries = make([]encoded, 0, bufSize)
	)

	done := make(chan interface{})
	go func() {
		for entry := range outputs {
			entries = append(entries, entry)
		}
		done <- struct{}{}
	}()

	walkCtx, walkErrs := db.walkAsync(key, inputs)
	readCtx, readErrs := db.readAsync(routines, inputs, outputs)
	<-walkCtx.Done()
	<-readCtx.Done()

	if err, ok := <-readErrs; !ok && err != nil {
		return nil, err
	}

	if err, ok := <-walkErrs; !ok && err != nil {
		return nil, err
	}

	<-done
	return entries, nil
}

func (db *DB) walkAsync(key Key, paths chan<- string) (context.Context, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() {
		defer cancel()
		defer close(paths)
		defer close(errs)
		err := filepath.Walk(key.Prefix(), func(path string, info fs.FileInfo, e error) error {
			if e != nil {
				return e
			}
			if info.IsDir() || !strings.HasSuffix(path, Extension) {
				return nil
			}
			paths <- path
			return nil
		})
		if err != nil {
			errs <- err
		}
		return
	}()
	return ctx, errs
}

func (db *DB) readAsync(routines int, paths <-chan string, entries chan<- encoded) (context.Context, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, routines)
	wg := &sync.WaitGroup{}
	wg.Add(routines)

	go func() {
		wg.Wait()
		close(errs)
		close(entries)
		cancel()
	}()

	for i := 0; i < routines; i++ {
		go func() {
			defer wg.Done()
			for p := range paths {
				bs, err := ioutil.ReadFile(p)
				if err != nil {
					errs <- err
					return
				}
				entries <- encoded{
					path:  p,
					bytes: bs,
				}
			}
		}()
	}

	return ctx, errs
}

func (db *DB) parseKey(s string) Key {
	return Key{
		prefix: strings.TrimSuffix(s, Extension),
		doc:    s,
	}
}

func (db *DB) Key(head string, tail ...string) Key {
	k := make([]string, 0, 2+len(tail))
	k = append(k, db.dataDir)
	k = append(k, head)
	k = append(k, tail...)
	prefix := path.Join(k...)
	doc := prefix + Extension
	return Key{
		prefix: prefix,
		doc:    doc,
	}
}

type Key struct {
	prefix string
	doc    string
}

func (k *Key) Prefix() string   { return k.prefix }
func (k *Key) Document() string { return k.doc }

type encoded struct {
	path  string
	bytes []byte
}

type decoded struct {
	key   Key
	value interface{}
}
