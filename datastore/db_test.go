package datastore

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	assert.Nil(t, err, err)
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, 1000)
	assert.Nil(t, err, err)
	defer db.Close()

	pairs := [][]string {
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}
	outFile, err := os.Open(filepath.Join(dir, outFileName))
	assert.Nil(t, err, err)

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			assert.Nil(t, err, "Cannot put %s: %s", pairs[0], err)

			value, err := db.Get(pair[0])
			assert.Nil(t, err, "Cannot get %s: %s", pairs[0], err)
			assert.Equal(t, pair[1], value, "Bad value returned expected %s, got %s", pair[1], value)
		}
	})

	outInfo, err := outFile.Stat()
	assert.Nil(t, err, err)
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			assert.Nil(t, err, "Cannot put %s: %s", pairs[0], err)
		}
		outInfo, err := outFile.Stat()
		assert.Nil(t, err, err)
		assert.Equal(t, size1 * 2, outInfo.Size(), "Unexpected size (%d vs %d)", size1, outInfo.Size())
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir, 1000)
		assert.Nil(t, err, err)

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			assert.Nil(t, err, "Cannot get %s: %s", pairs[0], err)
			assert.Equal(t, pair[1], value, "Bad value returned expected %s, got %s", pair[1], value)
		}
	})
}

func TestSegments (t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	assert.Nil(t, err, err)
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, 80)
	assert.Nil(t, err, err)
	defer db.Close()

	segment := []string{"key", "Lorem, ipsum dolor.Lorem, ipsum dolor.Lorem, ipsum dolor.Lorem, ipsum dolor. Lorem, ipsum dolor.Lorem, ipsum dolor.Lorem, ipsum dolor.Lorem, ipsum dolor."}

	t.Run("create segments", func(t *testing.T) {
		err = db.Put(segment[0], segment[1])
		assert.Nil(t, err, err)
		assert.Equal(t, 1, len(db.segments), "segments` index has wrong length expected %d, got %d", 1, len(db.segments))
		filePath := path.Join(db.dir, "0")
		_, err = os.Stat(filePath)
		assert.Nil(t, err, "segment file wasn't created")
	})

	t.Run("merge segments", func(t *testing.T) {
		err = db.Put(segment[0], segment[1])	// add segment
		assert.Nil(t, err, err)
		time.Sleep(time.Millisecond*100) // wait merge
		assert.Equal(t, 1, len(db.segments), "index has wrong length expected %d, got %d", 1, len(db.segments))
		filePath := db.getSPath(1)
		_, err = os.Stat(filePath)
		assert.NotNil(t, err, "segments` files wasn`t merged")
	})

	pairs := [][]string {
		{"a", "a1"},
		{"b", "b1"},
		{"c", "c1"},
		segment,
		{"a", "a2"},
		segment,
		{"b", "b2"},
	}

	assertPairs := [][]string {
		{"a", "a2"},
		{"b", "b2"},
		{"c", "c1"},
	}

	t.Run("Put/Get with merging", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			assert.Nil(t, err, "Cannot put %s: %s", pairs[0], err)
		}
	
		// before merging
		for _, pair := range assertPairs {
			value, err := db.Get(pair[0])
			assert.Nil(t, err, "Cannot get %s: %s", pairs[0], err)
			assert.Equal(t, pair[1], value, "Bad value returned expected %s, got %s", pair[1], value)
		}
	
		time.Sleep(time.Millisecond*100)

		//after merging
		assert.Equal(t, 1, len(db.segments), "segments` hashIndexes wasn`t merged")
		_, err := os.Stat(db.getSPath(1))
		assert.NotNil(t, err, "segments` files wasn`t merged")
		for _, pair := range assertPairs {
			value, err := db.Get(pair[0])
			assert.Nil(t, err, "Cannot get %s: %s", pairs[0], err)
			assert.Equal(t, pair[1], value, "Bad value returned expected %s, got %s", pair[1], value)
		}
	})

	t.Run("new db process: recover segments", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir, 100)
		assert.Nil(t, err, err)

		for _, pair := range assertPairs {
			value, err := db.Get(pair[0])
			assert.Nil(t, err, "Cannot get %s: %s", pairs[0], err)
			assert.Equal(t, pair[1], value, "Bad value returned expected %s, got %s", pair[1], value)
		}
	})
}