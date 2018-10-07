package gotinydb

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"reflect"
	"testing"

	"github.com/dgraph-io/badger"
	"golang.org/x/crypto/blake2b"
)

func TestFiles(t *testing.T) {
	defer clean()
	err := open(t)
	if err != nil {
		return
	}

	// 300MB
	randBuff := make([]byte, 300*1000*1000)
	rand.Read(randBuff)

	fileID := "test file ID"

	n, err := db.PutFile(fileID, bytes.NewBuffer(randBuff))
	if err != nil {
		t.Error(err)
		return
	}

	if n != len(randBuff) {
		t.Errorf("expected write size %d but had %d", len(randBuff), n)
		return
	}

	randHash := blake2b.Sum256(randBuff)

	readBuff := bytes.NewBuffer(nil)
	err = db.ReadFile(fileID, readBuff)
	if err != nil {
		t.Error(err)
		return
	}

	readHash := blake2b.Sum256(readBuff.Bytes())

	if !reflect.DeepEqual(randHash, readHash) {
		t.Error("the saved file and the rand file are not equal")
		return
	}

	// Check the ids with chunk number are well generated
	err = db.Badger.View(func(txn *badger.Txn) error {
		storeID := db.buildFilePrefix(fileID, -1)

		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false

		it := txn.NewIterator(opt)
		defer it.Close()
		prevLastByte := -1
		for it.Seek(storeID); it.ValidForPrefix(storeID); it.Next() {
			lastByte := int(it.Item().Key()[len(it.Item().Key())-1:][0])
			if prevLastByte+1 != lastByte {
				if prevLastByte == 255 && lastByte != 0 {
					t.Errorf("generated incremental bytes is not good")
				}
			}
			prevLastByte = lastByte
		}

		return nil
	})
	if err != nil {
		t.Error(err)
		return
	}

	err = db.DeleteFile(fileID)
	if err != nil {
		t.Error(err)
		return
	}

	err = db.Badger.View(func(txn *badger.Txn) error {
		storeID := db.buildFilePrefix(fileID, -1)

		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false

		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Seek(storeID); it.ValidForPrefix(storeID); it.Next() {
			return fmt.Errorf("must be empty response")
		}

		return nil
	})
	if err != nil {
		t.Error(err)
		return
	}
}

func TestFilesMultipleWriteSameID(t *testing.T) {
	defer clean()
	err := open(t)
	if err != nil {
		return
	}

	// 20MB
	randBuff := make([]byte, 15*1000*1000)
	rand.Read(randBuff)

	fileID := "test file ID"

	n, err := db.PutFile(fileID, bytes.NewBuffer(randBuff))
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(randBuff) {
		t.Errorf("expected write size %d but had %d", len(randBuff), n)
		return
	}

	// New smaller file 5MB
	randBuff = make([]byte, 5*1000*1000)
	rand.Read(randBuff)

	n, err = db.PutFile(fileID, bytes.NewBuffer(randBuff))
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(randBuff) {
		t.Errorf("expected write size %d but had %d", len(randBuff), n)
		return
	}

	readBuff := bytes.NewBuffer(nil)
	err = db.ReadFile(fileID, readBuff)
	if err != nil {
		t.Error(err)
		return
	}

	randHash := blake2b.Sum256(randBuff)
	readHash := blake2b.Sum256(readBuff.Bytes())

	if !reflect.DeepEqual(randHash, readHash) {
		t.Error("the saved file and the rand file are not equal")
		return
	}
}
