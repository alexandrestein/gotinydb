package vars

import (
	"encoding/base64"
	"fmt"
	"os"

	"github.com/minio/highwayhash"
)

var (
	// FilePermission defines the database file permission
	FilePermission os.FileMode = 0740 // u -> rwx | g -> r-- | o -> ---

	// ErrWrongType defines the wrong type error
	ErrWrongType = fmt.Errorf("wrong type")
	// ErrNotFound defines error when the asked ID is not found
	ErrNotFound = fmt.Errorf("not found")
	// ErrEmptyID defines error when the given id is empty
	ErrEmptyID = fmt.Errorf("empty ID")

	// ErrTheResponseIsOver defines error when *ResponseQuery.One is called and all response has been returned
	ErrTheResponseIsOver = fmt.Errorf("the response has no more values")
)

// buildID builds an ID as a slice of bytes from the given string
func buildID(id string) []byte {
	key := make([]byte, highwayhash.Size)
	hash := highwayhash.Sum128([]byte(id), key)
	return []byte(hash[:])
}

// BuildID returns ID as base 64 representation into a string
func BuildID(id string) string {
	return base64.RawURLEncoding.EncodeToString(buildID(id))
}

// BuildBytesID convert the given ID to an hash as byte represention
func BuildBytesID(id string) []byte {
	return []byte(BuildID(id))
}
