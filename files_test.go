package gotinydb

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
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

	// Change the file size from 5MB to 100KB
	defaultFileChuckSize := fileChuckSize
	fileChuckSize = 100 * 1000
	defer func(defaultFileChuckSize int) {
		fileChuckSize = defaultFileChuckSize
	}(defaultFileChuckSize)

	// 100MB
	randBuff := make([]byte, 100*1000*1000)
	rand.Read(randBuff)

	fileID := "test file ID"
	n, err := testDB.PutFile(fileID, "", bytes.NewBuffer(randBuff))
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
	err = testDB.ReadFile(fileID, readBuff)
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
	err = testDB.badger.View(func(txn *badger.Txn) error {
		storeID := testDB.buildFilePrefix(fileID, -1)

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

	err = testDB.DeleteFile(fileID)
	if err != nil {
		t.Error(err)
		return
	}

	err = testDB.badger.View(func(txn *badger.Txn) error {
		storeID := testDB.buildFilePrefix(fileID, -1)

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

	// ≊ 15MB
	randBuff := make([]byte, 15*999*1000)
	rand.Read(randBuff)

	fileID := "test file ID"

	n, err := testDB.PutFile(fileID, "", bytes.NewBuffer(randBuff))
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(randBuff) {
		t.Errorf("expected write size %d but had %d", len(randBuff), n)
		return
	}

	// New smaller file of ≊ 5MB
	randBuff = make([]byte, 5*999*1000)
	rand.Read(randBuff)

	n, err = testDB.PutFile(fileID, "", bytes.NewBuffer(randBuff))
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(randBuff) {
		t.Errorf("expected write size %d but had %d", len(randBuff), n)
		return
	}

	readBuff := bytes.NewBuffer(nil)
	err = testDB.ReadFile(fileID, readBuff)
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

func TestFilesReaderInterface(t *testing.T) {
	defer clean()
	err := open(t)
	if err != nil {
		return
	}

	// ≊ 15MB
	randBuff := make([]byte, 15*999*1000)
	rand.Read(randBuff)

	fileID := "test file ID"

	n, err := testDB.PutFile(fileID, "", bytes.NewBuffer(randBuff))
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(randBuff) {
		t.Errorf("expected write size %d but had %d", len(randBuff), n)
		return
	}

	// Read into the middle
	interfaceReadAtTest(t, fileID, randBuff, 8484246, 500, 500)

	// Read to the end
	interfaceReadAtTest(t, fileID, randBuff, len(randBuff)-200, 500, 200)

	// Test seek
	reader, err := testDB.GetFileReader(fileID)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	reader.Seek(50, io.SeekStart)
	interfaceReadTestAfterSeek(t, reader, randBuff, 50, 100)
	reader.Seek(50, io.SeekCurrent)
	interfaceReadTestAfterSeek(t, reader, randBuff, 200, 100)
	reader.Seek(50, io.SeekEnd)
	interfaceReadTestAfterSeek(t, reader, randBuff, len(randBuff)-50, 50)
}

func interfaceReadAtTest(t *testing.T, fileID string, randBuff []byte, readStart, readLength, wantedN int) {
	reader, err := testDB.GetFileReader(fileID)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	p := make([]byte, readLength)
	var n int
	n, err = reader.ReadAt(p, int64(readStart))
	if err != nil {
		t.Fatal(err)
	}
	if n != wantedN {
		t.Fatalf("the number of reader bytes must be %d but had %d", wantedN, n)
	}

	randChunk := randBuff[readStart : readStart+wantedN]
	if !reflect.DeepEqual(randChunk, p[:wantedN]) {
		t.Fatal("the saved and retrived buffer must be equal but not")
	}
}

func interfaceReadTestAfterSeek(t *testing.T, reader *Reader, randBuff []byte, readStart, wantedN int) {
	p := make([]byte, 100)
	n, err := reader.Read(p)
	if err != nil {
		t.Fatal(err)
	}
	if n != wantedN {
		t.Fatalf("the number of reader bytes must be %d but had %d", wantedN, n)
	}

	randChunk := randBuff[readStart : readStart+wantedN]
	if !reflect.DeepEqual(randChunk, p[:wantedN]) {
		fmt.Println(randChunk, p)
		fmt.Println(readStart)
		t.Fatal("the saved and retrived buffer must be equal but not")
	}
}
