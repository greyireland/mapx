package syncmap

import (
	"context"
	"fmt"
	"github.com/greyireland/log"
	"github.com/redis/go-redis/v9"
	"time"
)

type RedisStore struct {
	redis *redis.Client
	key   string
}

func MustNewRedisStore(db *redis.Client, key string) PubSubStore {
	s, err := NewRedisStore(db, key)
	if err != nil {
		panic(err)
	}
	return s
}

func NewRedisStore(db *redis.Client, key string) (PubSubStore, error) {

	s := &RedisStore{
		redis: db,
		key:   key,
	}
	return s, nil
}

func (s *RedisStore) Close() error {
	return s.redis.Close()
}

func (s *RedisStore) Set(key, value string) error {
	return s.redis.HSet(context.TODO(), s.key, key, value).Err()
}

func (s *RedisStore) Get(key string) (string, error) {
	return s.redis.HGet(context.TODO(), s.key, key).Result()
}

func (s *RedisStore) Del(key string) error {
	err := s.redis.HDel(context.TODO(), s.key, key).Err()
	return err
}

func (s *RedisStore) Keys(pattern string, limit int, withvals bool) ([]string, []string, error) {
	start := time.Now()
	var data []string
	var cursor uint64
	var err error
	var keys []string
	var vals []string
	var count int
	for {
		now := time.Now()
		data, cursor, err = s.redis.HScan(context.TODO(), s.key, cursor, pattern, 1000).Result()
		if err != nil {
			log.Warn("HScan error", "err", err)
			break
		}
		for i := 0; i < len(data); i = i + 2 {
			keys = append(keys, data[i])
			vals = append(vals, data[i+1])
		}
		count += len(data) / 2
		log.Info(fmt.Sprintf("hscan %s", s.key), "count", count, "cursor", cursor, "time", time.Since(now))
		if cursor == 0 {
			break
		}
	}
	log.Infod(time.Minute, fmt.Sprintf("hscan %s finish", s.key), "total", count, "time", time.Since(start))

	return keys, vals, nil
}

func (s *RedisStore) Publish(topic string, msg string) error {
	return s.redis.Publish(context.TODO(), topic, msg).Err()
}

func (s *RedisStore) Subscribe(topic string, ch chan string) error {
	p := s.redis.Subscribe(context.TODO(), topic)

	go func() {
		for msg := range p.Channel() {
			ch <- msg.Payload
		}
	}()
	return nil
}
