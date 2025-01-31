package syncmap

import (
	"encoding/json"
	"github.com/greyireland/log"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/redis/go-redis/v9"
	"time"
)

const (
	OpSet = "set"
	OpDel = "del"
)

type SyncMap[V any] struct {
	key   string
	c     cmap.ConcurrentMap[string, V]
	db    PubSubStore
	ch    chan Item[V]
	isPub bool
}

func NewSyncMap[V any](db *redis.Client, key string, isPub bool) *SyncMap[V] {
	return NewSyncMapWithStore[V](MustNewRedisStore(db, key), key, isPub)

}

func NewSyncMapWithStore[V any](db PubSubStore, key string, isPub bool) *SyncMap[V] {
	m := &SyncMap[V]{
		key:   key,
		db:    db,
		c:     cmap.New[V](),
		ch:    make(chan Item[V], 10000),
		isPub: isPub,
	}
	if !isPub {
		//subscribe
		go m.handleSync()
	}
	go m.fsync()
	err := m.load()
	if err != nil {
		panic(err)
	}
	return m
}

// handle data
func (m *SyncMap[V]) handleSync() {
	log.Info("start subscribe", "topic", m.topic())
	ch := make(chan string, 10000)
	err := m.db.Subscribe(m.topic(), ch)
	if err != nil {
		log.Warn("subscribe err", "err", err, "topic", m.topic())
		return
	}
	for {
		select {
		case data := <-ch:
			var item Item[V]
			err = json.Unmarshal([]byte(data), &item)
			if err != nil {
				log.Warn("unmarshal err", "err", err, "data", data)
				continue
			}
			switch item.Op {
			case OpSet:
				m.c.Set(item.Key, item.Val)
				//log.Info("subscribe set", "key", item.Key, "val", item.Val)
			case OpDel:
				m.c.Remove(item.Key)
			}
		}
	}
}

// fsync ch Item
func (m *SyncMap[V]) fsync() {
	for {
		select {
		case data := <-m.ch:
			switch data.Op {
			case OpSet:
				buf, err := json.Marshal(data.Val)
				if err != nil {
					log.Warn("marshal err", "err", err, "Val", data.Val)
					continue
				}
				err = m.db.Set(data.Key, string(buf))
				if err != nil {
					log.Warn("set err", "err", err, "Key", data.Key, "Val", string(buf))
					continue
				}

			case OpDel:
				err := m.db.Del(data.Key)
				if err != nil {
					log.Warn("del err", "err", err, "Key", data.Key)
				}
			}
			//publish isPub op
			if m.isPub {
				item, err := json.Marshal(data)
				if err != nil {
					log.Warn("marshal err", "err", err, "Val", data.Val)
					continue
				}
				m.db.Publish(m.topic(), string(item))
			}

		}
	}
}
func (m *SyncMap[V]) load() error {
	start := time.Now()
	keys, vals, err := m.db.Keys("", 0, true)
	if err != nil {
		log.Warn("load map err", "err", err, "Key", m.key)
		return err
	}
	for i := 0; i < len(keys); i++ {
		var v V
		err = json.Unmarshal([]byte(vals[i]), &v)
		if err != nil {
			log.Warn("unmarshal err", "err", err, "Key", keys[i], "Val", vals[i])
			continue
		}
		m.c.Set(keys[i], v)
	}
	log.Info("load map finish", "Key", m.key, "count", m.c.Count(), "time", time.Since(start))
	return nil
}
func (m *SyncMap[V]) Set(key string, value V) {

	m.ch <- Item[V]{
		Op:  OpSet,
		Key: key,
		Val: value,
	}
	m.c.Set(key, value)
	return
}

// get topic
func (m *SyncMap[V]) topic() string {
	return m.key + "_topic"
}

type Item[V any] struct {
	Op  string
	Key string
	Val V
}

func (m *SyncMap[V]) Get(key string) (V, bool) {
	v, ok := m.c.Get(key)
	if !ok {
		return v, false
	}
	return v, true
}

// Has has Key
func (m *SyncMap[V]) Has(key string) bool {
	return m.c.Has(key)
}

func (m *SyncMap[V]) Del(key string) {
	m.ch <- Item[V]{
		Op:  OpDel,
		Key: key,
	}
	m.c.Remove(key)
	return
}

func (m *SyncMap[V]) Count() int {
	return m.c.Count()
}

// Items get all items
func (m *SyncMap[V]) Items() map[string]V {
	return m.c.Items()
}
