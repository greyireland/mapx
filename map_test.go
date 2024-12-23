package mapx

import (
	"fmt"
	"testing"
	"time"
)

func TestNewKVMap(t *testing.T) {
	kv := NewMap[time.Time]("/tmp/test.db")
	now := time.Now()
	kv.Set("key1", time.Now())
	fmt.Println("set", time.Now().Sub(now))
	now = time.Now()
	v, _ := kv.Get("key1")

	fmt.Println(v, time.Now().Sub(now))
}
