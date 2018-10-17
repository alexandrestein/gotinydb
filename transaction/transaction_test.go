package transaction

import (
	"context"
	"reflect"
	"testing"
	"time"
)

var (
	key = []byte("key")
	val = []byte("val")
)

func TestAll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tx := NewTransaction(ctx, key, val, false)
	if tx == nil {
		t.Fatalf("tx is nil")
	}

	go func (ch chan error)  {
		<-ch
	}(tx.ResponseChan)

time.Sleep(time.Millisecond*10)

	if tx.Delete == true {
		t.Fatalf("tx should not be a deletation")
	}

	if !reflect.DeepEqual(tx.Value, val) {
		t.Fatalf("value is not good")
	}
	if !reflect.DeepEqual(tx.DBKey, key) {
		t.Fatalf("key is not good")
	}

	select {
	case tx.ResponseChan <- nil:
	default:
		t.Fatalf("chanel is not open")
	}

	tx = NewTransaction(ctx, key, val, true)
	if tx == nil {
		t.Fatalf("tx is nil")
	}

	if tx.Delete == false {
		t.Fatalf("tx should be a deletation")
	}
	if tx.Value != nil {
		t.Fatalf("tx value must be nil")
	}
}
