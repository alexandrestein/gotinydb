package gotinydb

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"

	"golang.org/x/crypto/blake2b"
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
func buildSelectorHash(selector []string) uint16 {
	hasher, _ := blake2b.New256(nil)
	for _, filedName := range selector {
		hasher.Write([]byte(filedName))
	}

	hash := binary.BigEndian.Uint16(hasher.Sum(nil))
	return hash
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

func deriveKey(key [32]byte, id, seed []byte) (cipherKey, nonce []byte) {
	hasher, _ := blake2b.New256(key[:])
	hasher.Write(id)
	cipherKey = hasher.Sum(nil)
	hasher.Write(seed)
	nonce = hasher.Sum(nil)
	nonce = nonce[:chacha20poly1305.NonceSizeX]
	return
}

func encrypt(key [32]byte, id, content []byte) []byte {
	seed := make([]byte, chacha20poly1305.NonceSizeX)
	rand.Read(seed)

	cipherKey, nonce := deriveKey(key, id, seed)
	aead, _ := chacha20poly1305.NewX(cipherKey)

	return append(seed, aead.Seal(nil, nonce, content, nil)...)
}

func decrypt(key [32]byte, id, content []byte) ([]byte, error) {
	seed := content[:chacha20poly1305.NonceSizeX]
	cipherKey, nonce := deriveKey(key, id, seed)
	aead, _ := chacha20poly1305.NewX(cipherKey)

	decrypedContent, err := aead.Open(nil, nonce, content[chacha20poly1305.NonceSizeX:], nil)
	if err != nil {
		return nil, err
	}

	return decrypedContent, nil
}
