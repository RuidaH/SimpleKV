package main

import (
	"github.com/simpleKV/db"
)

func main() {
	record := db.NewRecord([]byte("abc"), []byte("kk"))
	db.DPrintf("The original record: %v", *record)
	encodedRecord, _ := record.Encode()
	newRecord, _ := db.Decode(encodedRecord)
	db.DPrintf("The new record: %v", *newRecord)
	return
}
