package db

import "sync"

type Element struct {
	fileId       int
	recordSize   int64
	recordOffset int64
	timestamp    int64
}

type KeyDir struct {
	mu      sync.RWMutex
	indices map[string]Element
}

func NewKeyDir() *KeyDir {
	return &KeyDir{
		indices: make(map[string]Element),
	}
}

// what you should return as the value -> string?
func (kd *KeyDir) Get(key string) {

}
