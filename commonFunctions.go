package gotinydb

import (
	"context"
	"crypto/rand"
	"encoding/json"

	"github.com/minio/highwayhash"
	"golang.org/x/crypto/chacha20poly1305"
)

func getIDsAsString(input []*idType) (ret []string) {
	for _, id := range input {
		ret = append(ret, id.ID)
	}
	return ret
}

func newTransactionElement(id string, content interface{}, isInsertion bool, col *Collection) (wtElem *writeTransactionElement) {
	wtElem = &writeTransactionElement{
		id: id, contentInterface: content, isInsertion: isInsertion, collection: col,
	}

	if !isInsertion {
		return
	}

	if bytes, ok := content.([]byte); ok {
		wtElem.bin = true
		wtElem.contentAsBytes = bytes
	}

	if !wtElem.bin {
		jsonBytes, marshalErr := json.Marshal(content)
		if marshalErr != nil {
			return nil
		}

		wtElem.contentAsBytes = jsonBytes
	}

	return
}

func newFileTransactionElement(id string, chunkN int, content []byte, isInsertion bool) *writeTransactionElement {
	return &writeTransactionElement{
		id: id, chunkN: chunkN, contentAsBytes: content, isInsertion: isInsertion, isFile: true,
	}
}

func newTransaction(ctx context.Context) *writeTransaction {
	wt := new(writeTransaction)
	wt.ctx = ctx
	wt.responseChan = make(chan error, 0)

	return wt
}

func (wt *writeTransaction) addTransaction(trElement ...*writeTransactionElement) {
	wt.transactions = append(wt.transactions, trElement...)
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

func deriveKey(key, id []byte) (cipherKey []byte) {
	derived := highwayhash.Sum(id, key)
	return derived[:]
}

func encrypt(key, id, content []byte) []byte {
	aead, _ := chacha20poly1305.NewX(deriveKey(key, id))

	nonce := make([]byte, aead.NonceSize())
	rand.Read(nonce)

	return append(nonce, aead.Seal(nil, nonce, content, nil)...)
}

func decrypt(key, id, content []byte) ([]byte, error) {
	aead, _ := chacha20poly1305.NewX(deriveKey(key, id))

	decrypedContent, err := aead.Open(nil, content[:aead.NonceSize()], content[aead.NonceSize():], nil)
	if err != nil {
		return nil, err
	}

	return decrypedContent, nil
}
