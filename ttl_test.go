package gotinydb

import (
	"bytes"
	"testing"
	"time"
)

func TestTTLDocument(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
		return
	}

	defer clean()
	err := openT(t)
	if err != nil {
		return
	}

	err = testCol.PutWithTTL("test TTL ID", struct{}{}, time.Millisecond*500)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(time.Second)

	dest := struct{}{}
	_, err = testCol.Get("test TTL ID", &dest)
	if err == nil {
		t.Error("it must return an error but not")
		return
	}

	err = testCol.Put("test NO TTL ID", struct{}{})
	if err != nil {
		t.Error(err)
		return
	}

	err = testCol.PutWithTTL("test TTL ID 1", struct{}{}, time.Millisecond*500)
	if err != nil {
		t.Error(err)
		return
	}
	err = testCol.PutWithTTL("test TTL ID 20", struct{}{}, time.Millisecond*1000)
	if err != nil {
		t.Error(err)
		return
	}
	err = testCol.PutWithTTL("test TTL ID 21", struct{}{}, time.Millisecond*1000)
	if err != nil {
		t.Error(err)
		return
	}
	err = testCol.PutWithTTL("test TTL ID 22", struct{}{}, time.Millisecond*1000)
	if err != nil {
		t.Error(err)
		return
	}
	err = testCol.PutWithTTL("test TTL ID 23", struct{}{}, time.Millisecond*1000)
	if err != nil {
		t.Error(err)
		return
	}
	err = testCol.PutWithTTL("test TTL ID 3", struct{}{}, time.Millisecond*2500)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(time.Second * 3)

	_, err = testCol.Get("test TTL ID1", &dest)
	if err == nil {
		t.Error("it must return an error but not")
		return
	}
	_, err = testCol.Get("test TTL ID20", &dest)
	if err == nil {
		t.Error("it must return an error but not")
		return
	}
	_, err = testCol.Get("test TTL ID21", &dest)
	if err == nil {
		t.Error("it must return an error but not")
		return
	}
	_, err = testCol.Get("test TTL ID22", &dest)
	if err == nil {
		t.Error("it must return an error but not")
		return
	}
	_, err = testCol.Get("test TTL ID23", &dest)
	if err == nil {
		t.Error("it must return an error but not")
		return
	}
	_, err = testCol.Get("test TTL ID3", &dest)
	if err == nil {
		t.Error("it must return an error but not")
		return
	}

	_, err = testCol.Get("test NO TTL ID", &dest)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestTTLFile(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
		return
	}

	defer clean()
	err := openT(t)
	if err != nil {
		return
	}

	buffer := bytes.NewBufferString("this is the text file content")
	_, err = testDB.GetFileStore().PutFileWithTTL("test TTL file", "txt.txt", buffer, time.Millisecond*200)
	if err != nil {
		t.Error(err)
		return
	}

	var w Writer
	w, err = testDB.GetFileStore().GetFileWriterWithTTL("file writer with TTL ID", "test.txt", time.Millisecond*400)
	if err != nil {
		t.Error(err)
		return
	}
	defer w.Close()

	_, err = w.Write([]byte("This is the text file content"))
	if err != nil {
		t.Error(err)
		return
	}
	err = w.Close()
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(time.Millisecond * 1500)

	_, err = testDB.GetFileStore().GetFileReader("test TTL file")
	if err == nil {
		t.Error(err)
	}

	_, err = testDB.GetFileStore().GetFileReader("file writer with TTL ID")
	if err == nil {
		t.Error(err)
	}
}
