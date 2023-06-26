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

	var errorInfo error = nil
	if len(key) == 0 {
		errorInfo = errors.New("The length of key is 0.")
	}

	valueSet, ok := kd.indices[key]
	if !ok {
		errorInfo = errors.New("Key does not exist.")
	}
	return valueSet, errorInfo
}

// set or update the key-value pair in the hash table
func (kd *KeyDir) Put(key string, valueSet *ValueSet) error {
	kd.mu.Lock()
	defer kd.mu.Unlock()

	if len(key) == 0 {
		return errors.New("The length of key is 0.")
	}

	kd.indices[key] = *valueSet
	return nil
}

// delete the key-value pair in the hash table
func (kd *KeyDir) Del(key string) error {
	kd.mu.Lock()
	defer kd.mu.Unlock()

	if len(key) == 0 {
		return errors.New("The length of key is 0.")
	}

	if _, ok := kd.indices[key]; !ok {
		return errors.New("Key does not exist.")
	}
	delete(kd.indices, key)
	return nil
}
