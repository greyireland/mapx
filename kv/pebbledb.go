package kv

import (
	"bytes"
	"github.com/cockroachdb/pebble"
	"io"
)

type pebbleStore struct {
	db *pebble.DB
	wo *pebble.WriteOptions
}

func MustNewPebbleStore(path string, fsync bool) Store {
	s, err := NewPebbleStore(path, fsync)
	if err != nil {
		panic(err)
	}
	return s

}
func NewPebbleStore(path string, fsync bool) (Store, error) {

	opts := &pebble.Options{}
	if !fsync {
		opts.DisableWAL = true
	}

	wo := &pebble.WriteOptions{}
	wo.Sync = fsync

	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}

	return &pebbleStore{
		db: db,
		wo: wo,
	}, nil
}

func (s *pebbleStore) Close() error {
	s.db.Close()
	return nil
}

func (s *pebbleStore) PSet(keys, vals [][]byte) error {
	wb := s.db.NewBatch()

	for i, k := range keys {
		wb.Set(k, vals[i], s.wo)
	}
	return wb.Commit(s.wo)
}

func (s *pebbleStore) PGet(keys [][]byte) ([][]byte, error) {
	var vals = make([][]byte, len(keys))

	var err error
	var closer io.Closer
	for i, k := range keys {
		vals[i], closer, err = s.db.Get(k)
		if err != nil {
			continue
		}
		closer.Close()
	}
	return vals, err
}

func (s *pebbleStore) Set(key, value []byte) error {
	return s.db.Set(key, value, s.wo)
}

func (s *pebbleStore) Get(key []byte) ([]byte, error) {
	v, closer, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}
	closer.Close()
	return v, err
}

func (s *pebbleStore) Del(key []byte) error {
	err := s.db.Delete(key, s.wo)
	return err
}

func (s *pebbleStore) Keys(pattern []byte, limit int, withvals bool) ([][]byte, [][]byte, error) {
	var keys [][]byte
	var vals [][]byte

	io := &pebble.IterOptions{}
	it, _ := s.db.NewIter(io)
	defer it.Close()
	it.SeekGE(pattern)

	for ; it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, pattern) {
			break
		}

		k := it.Key()
		keys = append(keys, k)

		if withvals {
			value := it.Value()
			vals = append(vals, value)
		}
	}

	return keys, vals, nil
}

func (s *pebbleStore) FlushDB() error {
	return s.db.Flush()
}
