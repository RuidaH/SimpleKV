package db

import (
	"os"
	"sync"
)

type SimpleKV struct {
	mu       sync.Mutex       // Lock to protect shared data
	indices  map[string]int64 // indexing
	dirPath  string           // path to store data file
	dbFile   *DBFile          // keep track of active data file
	hintFile *os.File         // store info to speed up the reboot
}


// get
// put
// del
// loading indices
// close
// open
// merge