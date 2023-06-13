package datastore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
)

const outFileName = "current-data"

var ErrNotFound = fmt.Errorf("record does not exist")

type mergeItem struct {
	path   string
	offset int64
}

type mergeIndex map[string]mergeItem

type hashIndex map[string]int64

type Db struct {
	out       *os.File
	dir       string
	outPath   string
	outOffset int64
	mu        sync.Mutex
	limit     int64
	index     hashIndex
	segments  []hashIndex
	segCh     chan hashIndex
	putCh     chan entry
	putRes		chan error
}

func NewDb(dir string, segmLimit int64) (*Db, error) {
	outputPath := filepath.Join(dir, outFileName)
	os.MkdirAll(dir, 0o600)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	db := &Db{
		outPath: outputPath,
		out:     f,
		dir:     dir,
		index:   make(hashIndex),
		limit:   segmLimit,
		segCh:   make(chan hashIndex),
		putCh:   make(chan entry),
		putRes:  make(chan error),
	}
	err = db.recover()
	go db.merger(db.segCh)
	go db.putRoutine(db.putCh)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return db, nil
}

const bufSize = 8192

func (db *Db) recover() error {
	_, err := os.Stat(db.outPath)
	if err == nil {
		index, offset, err := recoverFile(db.outPath)
		if err != nil && err != io.EOF {
			return err
		}
		db.index = index
		db.outOffset = offset
	}

	for i := 0; ; i++ {
		_, err := os.Stat(db.getSPath(i))
		if err != nil {
			break
		}
		index, _, err := recoverFile(db.getSPath(i))
		if err != nil && err != io.EOF {
			return err
		}
		db.segments = append(db.segments, index)
	}
	return nil
}

func recoverFile(path string) (hashIndex, int64, error) {
	input, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer input.Close()

	index := make(hashIndex)
	var offset int64
	var buf [bufSize]byte
	in := bufio.NewReaderSize(input, bufSize)
	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return index, offset, err
			}
		} else if err != nil {
			return nil, 0, err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == nil {
			if n != int(size) {
				return nil, 0, fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			index[e.key] = offset
			offset += int64(n)
		}
	}
	return index, offset, err
}

func (db *Db) Close() error {

	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	outPath := db.outPath
	position, ok := db.index[key]
	if !ok {
		outPath, position, ok = db.getFromSegments(key)
		if !ok {
			return "", ErrNotFound
		}
	}

	file, err := os.Open(outPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}
	reader := bufio.NewReader(file)
	record, err := readRecord(reader)
	ok = checkHash(record)
	if !ok {
		return "", errors.New("wrong hash sum")
	}
	value := readValue(record)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *Db) getFromSegments(key string) (string, int64, bool) {
	var (
		outPath  string
		position int64
		ok       bool = false
	)
	if len(db.segments) == 0 {
		return "", 0, false
	}
	for i := len(db.segments) - 1; i >= 0; i-- {
		position, ok = db.segments[i][key]
		if ok {
			outPath = db.getSPath(i)
			break
		}
	}
	return outPath, position, ok
}

func (db *Db) Put(key, value string) (error) {
	e := entry{
		key:   key,
		value: value,
	}
	db.putCh <- e
	return <- db.putRes
}

func (db *Db) putRoutine(ch chan entry) {
	for {
		e := <-ch
		db.mu.Lock()
		n, err := db.out.Write(e.Encode())
		if err != nil {
			db.mu.Unlock()
			db.putRes <- err
			continue
		}
		db.index[e.key] = db.outOffset
		db.outOffset += int64(n)
		db.mu.Unlock()
		if db.outOffset > db.limit {
			err = db.addSegment()
		}
		db.putRes <- err
	}
}

func (db *Db) addSegment() error {
	db.mu.Lock()
	db.Close()
	newSegmentPath := db.getSPath(len(db.segments))
	err := os.Rename(db.outPath, newSegmentPath)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	f, err := os.OpenFile(db.outPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	db.out = f
	db.outOffset = 0
	db.segments = append(db.segments, db.index)
	db.index = make(hashIndex)
	db.mu.Unlock()
	if len(db.segments) > 1 {
		db.segCh <- db.segments[0]
		db.segCh <- db.segments[1]
	}
	return nil
}

func (db *Db) getSPath(index int) string {
	segName := strconv.Itoa(index)
	segPath := path.Join(db.dir, segName)
	return segPath
}
