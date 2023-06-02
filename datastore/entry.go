package datastore

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

type entry struct {
	key, value string
}

func (e *entry) Encode() []byte {
	data := []byte(e.key + e.value)
	hasher := sha256.New()
	hasher.Write(data)
	hashSum := hasher.Sum(nil)
	hl := len(hashSum)
	kl := len(e.key)
	vl := len(e.value)
	size := kl + vl + hl + 16
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	binary.LittleEndian.PutUint32(res[kl+8:], uint32(vl))
	copy(res[kl+12:], e.value)
	binary.LittleEndian.PutUint32(res[kl+vl+12:], uint32(hl))
	copy(res[kl+vl+16:], hashSum)
	return res
}

func (e *entry) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	e.key = string(keyBuf)

	vl := binary.LittleEndian.Uint32(input[kl+8:])
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+12:kl+12+vl])
	e.value = string(valBuf)
}

func checkHash(input []byte) bool {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])

	vl := binary.LittleEndian.Uint32(input[kl+8:])
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+12:kl+12+vl])

	data := append(keyBuf, valBuf...)
	hasher := sha256.New()
	hasher.Write(data)
	counted := hasher.Sum(nil)

	hl := binary.LittleEndian.Uint32(input[kl+12+vl:])
	hashBuf := make([]byte, hl)
	copy(hashBuf, input[kl+vl+16:kl+vl+16+hl])

	if bytes.Equal(hashBuf, counted) {
		return true
	}
	return false
}

func readValue(input []byte) string {
	kl := int(binary.LittleEndian.Uint32(input[4:]))
	vl := int(binary.LittleEndian.Uint32(input[kl+8:]))
	data := make([]byte, vl)
	copy(data, input[kl+12:kl+12+vl])
	return string(data)
}

func readRecord(in *bufio.Reader) ([]byte, error) {
	header, err := in.Peek(4)
	if err != nil {
		return nil, err
	}
	len := int(binary.LittleEndian.Uint32(header[0:]))
	data := make([]byte, len)
	n, err := in.Read(data)
	if err != nil {
		return nil, err
	}
	if n != len {
		return nil, fmt.Errorf("can't read value bytes (read %d, expected %d)", n, len)
	}
	return data, nil
}
