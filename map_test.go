package mapx

import (
	"encoding/json"
	"fmt"
	"github.com/greyireland/log"
	"testing"
	"time"
)

type Data struct {
	Name string
	Age  int
}

func TestNewKVMap(t *testing.T) {
	log.Root().SetHandler(log.StdoutHandler)
	kv := NewMap[Data]("/tmp/test.db")
	now := time.Now()

	for i := 0; i < 100; i++ {
		//d := Data{Name: "test", Age: i}
		//kv.Set("key"+fmt.Sprint(i), d)
		//fmt.Println("set", d, time.Now().Sub(now))
		now = time.Now()
		v, _ := kv.Get("key" + fmt.Sprint(i))
		fmt.Println("get", v, time.Now().Sub(now))
	}
	//kv.db.FlushDB()

	time.Sleep(time.Second * 3)
}

// test json marshal
func TestMarshal(t *testing.T) {
	d := "1"
	b, _ := json.Marshal(d)
	fmt.Println(string(b))
	var d2 string
	json.Unmarshal(b, &d2)
	fmt.Println(d2)
}
