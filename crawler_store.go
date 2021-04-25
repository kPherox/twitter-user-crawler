package main

import (
	"encoding/binary"
	"os"
)

type CrawlerStore struct {
	bf *os.File
}

func NewCrawlerStore() (*CrawlerStore, error) {
	bf, err := os.OpenFile("last.id", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &CrawlerStore{bf: bf}, nil
}

func (cs *CrawlerStore) Close() error {
	return cs.bf.Close()
}

func (cs *CrawlerStore) GetLastOffset() int64 {
	var r int64
	if err := binary.Read(cs.bf, binary.LittleEndian, &r); err != nil {
		if _, err := os.Stat("twitter.db"); os.IsNotExist(err) {
			return 0
		}
		db, err := NewSQLite3("twitter.db")
		if err != nil {
			return 0
		}
		offset, _ := db.GetLast()
		db.Close()
		return offset
	}

	return r
}

func (cs *CrawlerStore) SetLastOffset(offset int64) error {
	cs.bf.Truncate(0)
	cs.bf.Seek(0, 0)
	return binary.Write(cs.bf, binary.LittleEndian, offset)
}
