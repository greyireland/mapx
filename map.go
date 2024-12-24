package mapx

import (
	"encoding/json"
	"github.com/greyireland/mapx/kv"
	cmap "github.com/orcaman/concurrent-map/v2"
)

const (
	OpSet = "set"
	OpDel = "del"
)

type Map[V any] struct {
	c  cmap.ConcurrentMap[string, V]
	s  kv.Store
	ch chan item[V]
}

func NewMap[V any](path string) *Map[V] {
	return NewMapWithStore[V](kv.MustNewPebbleStore(path, true))
}

// NewMapWithStore new map with store
func NewMapWithStore[V any](s kv.Store) *Map[V] {
	m := &Map[V]{
		s:  s,
		c:  cmap.New[V](),
		ch: make(chan item[V], 10000),
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
		case item := <-m.ch:
			switch item.op {
			case OpSet:
				buf, _ := json.Marshal(item.val)
				m.s.Set([]byte(item.key), buf)
			case OpDel:
				m.s.Del([]byte(item.key))
			}
		}
	}
}
func (m *Map[V]) load() error {
	keys, vals, err := m.s.Keys([]byte("*"), 0, true)
	if err != nil {
		return err
	}
	for i := 0; i < len(keys); i++ {
		var v V
		err = json.Unmarshal(vals[i], &v)
		if err != nil {
			continue
		}
		m.c.Set(string(keys[i]), v)
	}
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
