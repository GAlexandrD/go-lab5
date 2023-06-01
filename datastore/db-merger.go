package datastore

import (
	"bufio"
	"os"
	"path"
)

func (db *Db) merger(ch chan hashIndex) {
	for {
		seg1, seg2 := <-ch, <-ch
		path1 := db.getSPath(0)
		path2 := db.getSPath(1)
		mi := mergeHashIndex(seg1, seg2, path1, path2)
		mergedPath := path.Join(db.dir, "merged")
		index, err := mergeFiles(mi, mergedPath)
		if err != nil {
			println("error occured during merging:", err.Error())
			os.RemoveAll(mergedPath)
			continue
		}
		db.mu.Lock()
		os.RemoveAll(path1)
		os.RemoveAll(path2)
		os.Rename(mergedPath, path1)
		if len(db.segments) > 2 {
			for i := 2; i <= len(db.segments)-1; i++ {
				db.segments[i-1] = db.segments[i]
				os.Rename(db.getSPath(i), db.getSPath(i-1))
			}
		}
		db.segments[0] = index
		db.segments = db.segments[:len(db.segments)-1]
		db.mu.Unlock()
	}
}

func mergeHashIndex(i1 hashIndex, i2 hashIndex, p1 string, p2 string) mergeIndex {
	mi := make(mergeIndex)
	for key, val := range i1 {
		mi[key] = mergeItem{path: p1, offset: val}
	}
	for key, val := range i2 {
		mi[key] = mergeItem{path: p2, offset: val}
	}
	return mi
}

func mergeFiles(mi mergeIndex, outPath string) (hashIndex, error) {
	f, err := os.Create(outPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	f.Chmod(0o600)

	var offset int64
	index := make(hashIndex)
	for key, value := range mi {
		file, err := os.Open(value.path)
		if err != nil {
			return nil, err
		}

		_, err = file.Seek(value.offset, 0)
		if err != nil {
			return nil, err
		}

		reader := bufio.NewReader(file)
		value, err := readValue(reader)
		if err != nil {
			return nil, err
		}
		file.Close()

		e := entry{
			key:   key,
			value: value,
		}
		n, err := f.Write(e.Encode())
		if err != nil {
			return nil, err

		}
		index[key] = offset
		offset += int64(n)
	}
	return index, nil
}
