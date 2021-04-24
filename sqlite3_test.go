package main

import (
	"testing"

	"io/ioutil"
	"os"
)

func TestNewSQLite3(t *testing.T) {
	tempFilename, err := tempFilename(t)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFilename)
	sqlite3, err := NewSQLite3(tempFilename)
	if err != nil {
		t.Fatal(err)
	}
	defer sqlite3.Close()
	rows, err := sqlite3.db.Query("PRAGMA table_info(user)")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols := make([]struct {
		cid        int
		name       string
		ctype      string
		notnull    int
		dflt_value *string
		pk         int
	}, 0, 2)
	for rows.Next() {
		col := struct {
			cid        int
			name       string
			ctype      string
			notnull    int
			dflt_value *string
			pk         int
		}{}
		err = rows.Scan(&col.cid, &col.name, &col.ctype, &col.notnull, &col.dflt_value, &col.pk)
		if err != nil {
			t.Fatal(err)
		}
		cols = append(cols, col)
	}

	var (
		hasId        bool
		hasCreatedAt bool
	)
	for _, c := range cols {
		if c.name == "id" && c.ctype == "INTEGER" && c.notnull > 0 && c.pk > 0 {
			hasId = true
			continue
		}
		if c.name == "created_at" && c.ctype == "timestamp" && c.notnull == 0 && c.pk == 0 {
			hasCreatedAt = true
			continue
		}
	}
	if !hasId || !hasCreatedAt {
		t.Fatal("mismatch user schema")
	}
}

func tempFilename(t *testing.T) (string, error) {
	f, err := ioutil.TempFile("", "test-")
	if err != nil {
		return "", err
	}
	f.Close()
	return f.Name(), nil
}
