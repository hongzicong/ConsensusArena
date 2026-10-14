package state

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
)

func (st *State) Preload(recordCount, valueSize int, seed uint64) (string, error) {
	if recordCount < 0 {
		return "", errors.New("record count must be non-negative")
	}
	if valueSize < 0 {
		return "", errors.New("value size must be non-negative")
	}

	st.mutex.Lock()
	defer st.mutex.Unlock()
	if st.Store.Size() != 0 {
		return "", errors.New("state must be empty before preload")
	}

	digest := sha256.New()
	var keyBytes [8]byte
	for key := 0; key < recordCount; key++ {
		value := make(Value, valueSize)
		fillPreloadedValue(value, uint64(key), seed)
		stateKey := Key(key)
		st.Store.Put(stateKey, value)
		binary.LittleEndian.PutUint64(keyBytes[:], uint64(stateKey))
		digest.Write(keyBytes[:])
		digest.Write(value)
	}

	return hex.EncodeToString(digest.Sum(nil)), nil
}

func fillPreloadedValue(value Value, key, seed uint64) {
	generatorState := seed ^ (key * 0x9e3779b97f4a7c15)
	for offset := 0; offset < len(value); {
		generatorState += 0x9e3779b97f4a7c15
		word := generatorState
		word = (word ^ (word >> 30)) * 0xbf58476d1ce4e5b9
		word = (word ^ (word >> 27)) * 0x94d049bb133111eb
		word ^= word >> 31
		for remaining := 0; remaining < 8 && offset < len(value); remaining++ {
			value[offset] = byte(word)
			word >>= 8
			offset++
		}
	}
}
