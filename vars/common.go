package vars

import (
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
)

// buildID builds an ID as a slice of bytes from the given string
func buildID(id string) []byte {
	key := make([]byte, highwayhash.Size)
	hash := highwayhash.Sum128([]byte(id), key)
	return []byte(hash[:])
}

// BuildIDAsString builds an ID of 128bits (16 bytes) as hexadecimal representation as string
func BuildIDAsString(id string) string {
	return fmt.Sprintf("%x", buildID(id))
}

func BuildIDAsBytes(id string) []byte {
	return []byte(BuildIDAsString(id))
}
