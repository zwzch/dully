package db

import (
	"testing"
	"fmt"
)

func Test_crc32(t *testing.T) {
	FDB := NewDB("../data/fsdb", true)
	//key:="xxx"
	//value:="value"
	//var bkey []byte = []byte(key)
	//var bvalue []byte = []byte(value)
	//fmt.Println(bkey)
	//fmt.Println(bvalue)
	//err := FDB.Put(bkey, bvalue)
	//if err == nil {
	//	fmt.Println(FDB.Get(bkey))
	//}
	//data, err :=FDB.Get(bkey)
	//if err == nil {
	//	fmt.Println(string(data[:]))
	//}
	//轮询leveldb中的数据

	iter := FDB.db.NewIterator(nil, nil)
	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		key := iter.Key()
		fmt.Println("key: ", string(key[:]))
		value := iter.Value()
		fmt.Println("value: ", string(value[:]))
	}



}