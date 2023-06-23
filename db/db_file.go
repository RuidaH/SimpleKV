package db

import "os"

type DBFile struct {
	file *os.File
	offset int64
}

