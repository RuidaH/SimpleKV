package db

import (
	"errors"
	"sync"
	"time"
)

type ValueSet struct {
	fileId       int
	recordSize   int64
	recordOffset int64
	timestamp    int64
}

type KeyDir struct {
	mu      sync.RWMutex
	indices map[string]ValueSet
}

func NewKeyDir() *KeyDir {
	return &KeyDir{
		indices: make(map[string]ValueSet),
	}
}

func NewValueSet(fileId int, size int64, offset int64) *ValueSet {
	return &ValueSet{
		fileId:       fileId,
		recordSize:   size,
		recordOffset: offset,
		timestamp:    time.Now().Unix(),
	}
}

// get the value set of the given key in the hash table
func (kd *KeyDir) Get(key string) (ValueSet, error) {
	kd.mu.RLock()
	defer kd.mu.RUnlock()

	valueSet, ok := kd.indices[key]
	var errorInfo error = nil
	if !ok {
		errorInfo = errors.New("Key " + key + " does not exist.")
	}
	return valueSet, errorInfo
}

// set or update the key-value pair in the hash table
func (kd *KeyDir) Put(key string, valueSet *ValueSet) {
	kd.mu.Lock()
	defer kd.mu.Unlock()

	kd.indices[key] = *valueSet
}

// delete the key-value pair in the hash table
func (kd *KeyDir) Del(key string) error {
	kd.mu.Lock()
	defer kd.mu.Unlock()

	if len(key) == 0 {
		return errors.New("The length of key is 0.")
	}

	if _, ok := kd.indices[key]; !ok {
		return errors.New("Key " + key + " does not exist.")
	}
	delete(kd.indices, key)
	return nil
}

func (val *ValueSet) GetFileId() int {
	return val.fileId
}

func (val *ValueSet) GetRecordSize() int64 {
	return val.recordSize
}

func (val *ValueSet) GetRecordOffset() int64 {
	return val.recordOffset
}
