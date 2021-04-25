package main

import (
	"log"
	"os"
)

type CrawlerStore struct {
	lf *os.File
}

func NewCrawlerStore() (*CrawlerStore, error) {
	lf, err := os.OpenFile("last.id", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &CrawlerStore{lf}, nil
}

func (cs *CrawlerStore) Close() error {
	return cs.lf.Close()
}

func (cs *CrawlerStore) GetLastOffset() int64 {
	data := make([]byte, 8)
	if _, err := cs.lf.Read(data); err != nil {
		if _, err := os.Stat("twitter.db"); os.IsNotExist(err) {
			return 0
		}
		db, err := NewSQLite3("twitter.db")
		if err != nil {
			log.Fatalf("Error: %s", err)
		}
		offset, _ := db.GetLast()
		db.Close()
		return offset
	}

	return int64(data[0]) |
		int64(data[1])<<8 |
		int64(data[2])<<16 |
		int64(data[3])<<24 |
		int64(data[4])<<32 |
		int64(data[5])<<40 |
		int64(data[6])<<48 |
		int64(data[7])<<56
}

func (cs *CrawlerStore) SetLastOffset(offset int64) (int, error) {
	cs.lf.Truncate(0)
	cs.lf.Seek(0, 0)
	buf := make([]byte, 8)
	buf[0] = byte(0xff & offset)
	buf[1] = byte(0xff & (offset >> 8))
	buf[2] = byte(0xff & (offset >> 16))
	buf[3] = byte(0xff & (offset >> 24))
	buf[4] = byte(0xff & (offset >> 32))
	buf[5] = byte(0xff & (offset >> 40))
	buf[6] = byte(0xff & (offset >> 48))
	buf[7] = byte(0xff & (offset >> 56))
	return cs.lf.Write(buf)
}
