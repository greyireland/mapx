package syncmap

import (
	"fmt"
	"github.com/greyireland/log"
	"github.com/redis/go-redis/v9"
	"testing"
	"time"
)

func TestNewSyncMap(t *testing.T) {
	log.Root().SetHandler(log.StdoutHandler)
	opts, err := redis.ParseURL("redis://@127.0.1:6379/0?protocol=3")
	if err != nil {
		panic(err)
	}
	opts.ReadTimeout = time.Second * 300
	opts.WriteTimeout = time.Second * 300
	opts.DialTimeout = time.Second * 300
	r := redis.NewClient(opts)
	w := NewSyncMap[string](r, "test", true)
	go func() {
		for i := 0; i < 10000; i++ {
			w.Set("key"+fmt.Sprint(i), "val"+fmt.Sprint(i))
			time.Sleep(time.Millisecond * 10)
		}

	}()
	time.Sleep(time.Second * 3)
	r2 := NewSyncMap[string](r, "test", false)

	for i := 0; i < 100; i++ {
		v, _ := r2.Get("key" + fmt.Sprint(i))
		fmt.Println("get", v)
		time.Sleep(time.Second)
	}
}
