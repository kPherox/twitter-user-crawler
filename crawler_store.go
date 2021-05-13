package main

import (
	"encoding/binary"
	"os"
)

type CrawlerStore struct {
	bf *os.File
	db *SQLite3
}

func NewCrawlerStore(filename string, isDatabase bool) (*CrawlerStore, error) {
	if isDatabase {
		return newCrawlerStoreDB(filename)
	}

	return newCrawlerStoreFile(filename)
}

func newCrawlerStoreDB(filename string) (cs *CrawlerStore, err error) {
	db, err := NewSQLite3(filename)
	if err != nil {
		return
	}
	cs = &CrawlerStore{db: db}
	return
}

func newCrawlerStoreFile(filename string) (cs *CrawlerStore, err error) {
	bf, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	cs = &CrawlerStore{bf: bf}
	return
}

func (cs *CrawlerStore) Close() error {
	if cs.db != nil {
		return cs.db.Close()
	}
	if cs.bf != nil {
		return cs.bf.Close()
	}
	return nil
}

func (cs *CrawlerStore) GetIDs(max int) [][]int64 {
	if cs.db != nil {
		return cs.getIDsFromDB(max)
	}
	if cs.bf != nil {
		return cs.getIDsFromFile(max)
	}
	return nil
}

func (cs *CrawlerStore) getIDsFromDB(max int) [][]int64 {
	ids, err := cs.db.GetIDs(int64(max) * 100)
	if err != nil {
		return nil
	}

	chunks := make([][]int64, 0, max)
	for i := 0; i < len(ids); i += 100 {
		end := i + 100
		if end > len(ids) {
			end = len(ids)
		}
		chunks = append(chunks, ids[i:end])
	}

	return chunks
}

func (cs *CrawlerStore) getIDsFromFile(max int) [][]int64 {
	offset := cs.getOffsetFromFile()

	chunks := make([][]int64, max)
	for ci := 0; ci < max; ci++ {
		ids := make([]int64, 100)
		for i := 0; i < 100; i++ {
			offset++
			ids[i] = offset
		}
		chunks[ci] = ids
	}

	return chunks
}

func (cs *CrawlerStore) getOffsetFromFile() int64 {
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
	if cs.db != nil {
		return cs.setOffsetFromDB(offset)
	}
	if cs.bf != nil {
		return cs.setOffsetFromFile(offset)
	}
	return nil
}

func (cs *CrawlerStore) setOffsetFromDB(offset int64) error {
	return cs.db.DeleteIDs(offset)
}

func (cs *CrawlerStore) setOffsetFromFile(offset int64) error {
	cs.bf.Truncate(0)
	cs.bf.Seek(0, 0)
	return binary.Write(cs.bf, binary.LittleEndian, offset)
}
