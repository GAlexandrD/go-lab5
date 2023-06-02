package datastore

import (
	"bufio"
	"bytes"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	e := entry{"key", "value"}
	e.Decode(e.Encode())
	if e.key != "key" {
		t.Error("incorrect key")
	}
	if e.value != "value" {
		t.Error("incorrect value")
	}
}

func TestReadValue(t *testing.T) {
	e := entry{"key", "test-value"}
	data := e.Encode()
	record, err := readRecord(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	value := readValue(record)
	if value != "test-value" {
		t.Errorf("Got bat value [%s]", value)
	}
}

func TestHashCheck(t *testing.T) {
	e := entry{"key", "test-value"}
	data := e.Encode()
	ok := checkHash(data)
	if !ok {
		t.Errorf("hashCheck returned false on valid record")
	}
	data[9] = 0
	ok = checkHash(data)
	if ok {
		t.Errorf("hashCheck passed corrupted data")
	}
}
