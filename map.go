package mapx

import (
	"encoding/json"
	"github.com/greyireland/log"
	"github.com/greyireland/mapx/kv"
	cmap "github.com/orcaman/concurrent-map/v2"
	"time"
)

const (
	OpSet = "set"
	OpDel = "del"
)

type Map[V any] struct {
	path string
	c    cmap.ConcurrentMap[string, V]
	db   kv.Store
	ch   chan item[V]
}

func NewMap[V any](path string) *Map[V] {
	return NewMapWithStore[V](path, kv.MustNewPebbleStore(path, true))
}

// NewMapWithStore new map with store
func NewMapWithStore[V any](path string, s kv.Store) *Map[V] {
	m := &Map[V]{
		path: path,
		db:   s,
		c:    cmap.New[V](),
		ch:   make(chan item[V], 10000),
	}
	err := m.load()
	if err != nil {
		panic(err)
	}
	go m.sync()
	return m
}

// sync ch item
func (m *Map[V]) sync() {
	for {
		select {
		case data := <-m.ch:
			switch data.op {
			case OpSet:
				buf, err := json.Marshal(data.val)
				if err != nil {
					log.Warn("marshal err", "err", err, "val", data.val)
					continue
				}
				err = m.db.Set([]byte(data.key), buf)
				if err != nil {
					log.Warn("set err", "err", err, "key", data.key, "val", string(buf))
					continue
				}
			case OpDel:
				err := m.db.Del([]byte(data.key))
				if err != nil {
					log.Warn("del err", "err", err, "key", data.key)
				}

			}
		}
	}
}
func (m *Map[V]) load() error {
	start := time.Now()
	keys, vals, err := m.db.Keys([]byte(""), 0, true)
	if err != nil {
		log.Warn("load map err", "err", err, "path", m.path)
		return err
	}
	for i := 0; i < len(keys); i++ {
		var v V
		err = json.Unmarshal(vals[i], &v)
		if err != nil {
			log.Warn("unmarshal err", "err", err, "key", string(keys[i]), "val", string(vals[i]))
			continue
		}
		m.c.Set(string(keys[i]), v)
	}
	log.Info("load map", "path", m.path, "count", m.c.Count(), "time", time.Since(start))
	return nil
}
func (m *Map[V]) Set(key string, value V) {

	m.ch <- item[V]{
		op:  OpSet,
		key: key,
		val: value,
	}
	m.c.Set(key, value)
	return
}

type item[V any] struct {
	op  string
	key string
	val V
}

func (m *Map[V]) Get(key string) (V, bool) {
	v, ok := m.c.Get(key)
	if !ok {
		return v, false
	}
	return v, true
}

// Has has key
func (m *Map[V]) Has(key string) bool {
	return m.c.Has(key)
}

func (m *Map[V]) Del(key string) {
	m.ch <- item[V]{
		op:  OpDel,
		key: key,
	}
	m.c.Remove(key)
	return
}

func (m *Map[V]) Count() int {
	return m.c.Count()
}

// Items get all items
func (m *Map[V]) Items() map[string]V {
	return m.c.Items()
}
