package main

import (
	"github.com/simpleKV/db"
)

func main() {
	kv, err := db.Open(db.DefaultDirPath)
	if err != nil {
		db.DPrintf("Error: %v", err)
	} else {
		db.DPrintf("The database %v has been created", kv)
	}

	kv.Put("a", "abcd")
	kv.Put("a", "cccc")
	kv.Put("b", "1234")
	kv.Put("c", "3456")
	kv.Put("d", "5676457")
	kv.Put("e", "ert")
	kv.Put("f", "23")
	key := "a"
	val, err := kv.Get(key)
	if err != nil {
		db.DPrintf("Error: %v", err)
	} else {
		db.DPrintf("The value of key %v: %v", key, val)
	}

	// delete a key
	deletedKey := "f"
	err1 := kv.Del(deletedKey)
	if err1 != nil {
		db.DPrintf("Error: %v", err1)
	} else {
		db.DPrintf("Key %v has been deleted", deletedKey)
	}

	// try to access the same key again
	key1 := "f"
	val1, err2 := kv.Get(key1)
	if err2 != nil {
		db.DPrintf("Error: %v", err2)
	} else {
		db.DPrintf("The value of key %v: %v", key1, val1)
	}

}

// record testing
func recordTest() {
	record := db.NewRecord([]byte("abc"), []byte("kk"))
	db.DPrintf("The original record: %v", *record)

	encodedRecord, _ := record.Encode()

	newRecord, _ := db.Decode(encodedRecord)
	db.DPrintf("The new record: %v", *newRecord)
}

// KeyDir Hash table testing
func hashTableTest() {
	dir := db.NewKeyDir()

	if _, err := dir.Get("aa"); err != nil {
		db.DPrintf("Error: %v", err)
	}

	key, valueSet := "aha", &db.ValueSet{}
	dir.Put(key, valueSet)

	if val, err := dir.Get(key); err != nil {
		db.DPrintf("Error: %v", err)
	} else {
		db.DPrintf("The valueSet of the key %v: %v", key, val)
	}

	// delete a key that doesn't exist
	if err := dir.Del("aa"); err != nil {
		db.DPrintf("Error: %v", err)
	} else {
		db.DPrintf("The key-value pair aa has been deleted")
	}

	db.DPrintf("The hash table: %v", dir)

	// delete a existed key
	if err := dir.Del(key); err != nil {
		db.DPrintf("Error: %v", err)
	} else {
		db.DPrintf("The key-value pair aa has been deleted")
	}

	db.DPrintf("The hash table: %v", dir)
}
