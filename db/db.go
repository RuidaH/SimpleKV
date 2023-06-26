package db

import (
	"os"
)

const (
	DataFileName       = "dataFile"
	MergeFileName      = "mergeFile"
	DataFileExtension  = ".seg"
	HintFileExtension  = ".hint"
	MergeFileExtension = ".merge"
)

type SimpleKV struct {
	kv          *KeyDir  // hash table that maps every key in a Bitcask to a fixed-size structure
	dirPath     string   // path to store data file
	numOfFiles  int      // keep track of the number of data files in DB
	curDataFile *DBFile  // keep track of active data file
	hintFile    *os.File // store info to speed up the reboot
}

func (kv *SimpleKV) Open() {

}

func (kv *SimpleKV) Close() {

}

func (kv *SimpleKV) LoadingIndices() {

}

func (kv *SimpleKV) Merge() {

}
