package main

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type SQLite3 struct {
	db *sql.DB
}

type UserModel struct {
	Id        int64
	CreatedAt time.Time
}

func NewSQLite3(filename string) (s *SQLite3, err error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared", filename))
	if err == nil {
		db.SetMaxOpenConns(1)
		s = &SQLite3{db}
		_, err = db.Exec("CREATE TABLE IF NOT EXISTS user (id INTEGER PRIMARY KEY NOT NULL, created_at timestamp)")
	}
	return
}

func (s *SQLite3) Close() error {
	return s.db.Close()
}

func (s *SQLite3) GetLast() (id int64, err error) {
	err = s.db.QueryRow("SELECT id FROM user ORDER BY id DESC LIMIT 1").Scan(&id)
	return
}

func (s *SQLite3) BulkInsert(us []UserModel) (err error) {
	query, ids := createBulkInsertQuery(us)
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return
	}
	defer stmt.Close()

	_, err = stmt.Exec(ids...)
	if err != nil {
		return
	}

	return
}

func createBulkInsertQuery(us []UserModel) (query string, ids []interface{}) {
	length := len(us)
	ids = make([]interface{}, length*2)
	vs := make([]string, length)
	for i, u := range us {
		vs[i] = "(?, ?)"
		ids[i*2] = u.Id
		ids[i*2+1] = u.CreatedAt.Unix()
	}
	query = fmt.Sprintf("INSERT OR REPLACE INTO user(id, created_at) VALUES %s", strings.Join(vs, ", "))
	return
}
