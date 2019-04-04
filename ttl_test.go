package gotinydb

import (
	"testing"
	"time"
)

func TestTTLStruct(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
		return
	}

	defer clean()
	err := open(t)
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
}
