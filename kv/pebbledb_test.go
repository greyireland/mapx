package kv

import (
	"fmt"
	"testing"
)

func TestMustNewPebbleStore(t *testing.T) {
	db, _ := NewPebbleStore("db.db", true)
	for i := 0; i < 100; i++ {

		//db.Set([]byte("key"+fmt.Sprint(i)), []byte("value"+fmt.Sprint(i)))
		v, err := db.Get([]byte("key" + fmt.Sprint(i)))
		fmt.Println("get", fmt.Sprint(v), err)
	}
	keys, vals, _ := db.Keys([]byte(""), 0, true)
	for i := 0; i < 100; i++ {
		fmt.Println("keys", string(keys[i]), "vals", string(vals[i]))
	}
}
