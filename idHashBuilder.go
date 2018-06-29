package gotinydb

import (
	"encoding/base64"

	"github.com/minio/highwayhash"
)

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
