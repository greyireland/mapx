package mapx

import (
	"fmt"
	"testing"
	"time"
)

func TestNewKVMap(t *testing.T) {
	kv := NewMap[uint64]("/tmp/test.db")
	now := time.Now()
	kv.Set("key1", 1)
	fmt.Println("set", time.Now().Sub(now))
	now = time.Now()
	v, _ := kv.Get("key1")

	fmt.Println(v, time.Now().Sub(now))
	time.Sleep(time.Second * 3)
}
