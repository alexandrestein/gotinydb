package cipher

import (
	"crypto/rand"
	"fmt"

	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/chacha20poly1305"
)

func deriveKey(key [32]byte, id, seed []byte) (cipherKey, nonce []byte) {
	hasher, _ := blake2b.New256(key[:])
	hasher.Write(id)
	cipherKey = hasher.Sum(nil)
	hasher.Write(seed)
	nonce = hasher.Sum(nil)
	nonce = nonce[:chacha20poly1305.NonceSizeX]
	return
}

// Encrypt derives the premary key with the given id and a random value.
// Returns the corresponding encrypted content with the random seed for derivation.
// The returned value can be decrypted by using the same key and id with Decrypt function.
func Encrypt(key [32]byte, id, content []byte) []byte {
	seed := make([]byte, chacha20poly1305.NonceSizeX)
	rand.Read(seed)

	cipherKey, nonce := deriveKey(key, id, seed)
	aead, _ := chacha20poly1305.NewX(cipherKey)

	return append(seed, aead.Seal(nil, nonce, content, nil)...)
}

// Decrypt derives the premary key with the given id and a random value.
// It reads the first bytes to get the derivation seed and tries to decrypt the content.
// Returns the aead.Open error if any.
func Decrypt(key [32]byte, id, content []byte) ([]byte, error) {
	if len(content) <= chacha20poly1305.NonceSizeX {
		return nil, ErrContentTooShort
	}

	seed := content[:chacha20poly1305.NonceSizeX]
	cipherKey, nonce := deriveKey(key, id, seed)
	aead, _ := chacha20poly1305.NewX(cipherKey)

	return aead.Open(nil, nonce, content[chacha20poly1305.NonceSizeX:], nil)
}

var (
	// ErrContentTooShort is returned when caller tries to decrypt a content which is too short
	ErrContentTooShort = fmt.Errorf("the content must be at least %d for decryption", chacha20poly1305.NonceSizeX)
)
