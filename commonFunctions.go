package gotinydb

import (
	"context"
	"encoding/base64"
	"sync"

	"github.com/minio/highwayhash"
)

// waitForDoneErrOrCanceled waits for the waitgroup or context to be done.
// If the waitgroup os done first it returns true otherways it returns false.
func waitForDoneErrOrCanceled(ctx context.Context, wg *sync.WaitGroup, errChan chan error) error {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return nil // completed normally
	case err := <-errChan:
		return err // returns the received err
	case <-ctx.Done():
		return ctx.Err() // timed out or canceled
	}
}

func getIDsAsString(input []*idType) (ret []string) {
	for _, id := range input {
		ret = append(ret, id.ID)
	}
	return ret
}

func newTransactionElement(id string, content interface{}) *writeTransactionElement {
	return &writeTransactionElement{
		id: id, contentInterface: content,
	}
}

func newTransaction(ctx context.Context) *writeTransaction {
	wt := new(writeTransaction)
	wt.ctx = ctx
	wt.responseChan = make(chan error, 0)

	return wt
}

func (wt *writeTransaction) addTransaction(trElement *writeTransactionElement) {
	wt.transactions = append(wt.transactions, trElement)
}

// buildIDInternal builds an ID as a slice of bytes from the given string
func buildIDInternal(id string) []byte {
	key := make([]byte, highwayhash.Size)
	hash := highwayhash.Sum128([]byte(id), key)
	return []byte(hash[:])
}

// buildID returns ID as base 64 representation into a string
func buildID(id string) string {
	return base64.RawURLEncoding.EncodeToString(buildIDInternal(id))
}

// buildBytesID convert the given ID to an hash as byte definition
func buildBytesID(id string) []byte {
	return []byte(buildID(id))
}

// buildSelectorHash returns a string hash of the selector
func buildSelectorHash(selector []string) uint64 {
	key := make([]byte, highwayhash.Size)
	hasher, _ := highwayhash.New64(key)
	for _, filedName := range selector {
		hasher.Write([]byte(filedName))
	}
	return hasher.Sum64()
}

// TypeName return the name of the type as a string
func (it IndexType) TypeName() string {
	switch it {
	case StringIndex:
		return "StringIndex"
	case IntIndex:
		return "IntIndex"
	case TimeIndex:
		return "TimeIndex"
	default:
		return ""
	}
}
