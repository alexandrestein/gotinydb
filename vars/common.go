package vars

import (
	"fmt"
	"os"

	"github.com/minio/highwayhash"
)

var (
	// FilePermission defines the database file permission
	FilePermission os.FileMode = 0740 // u -> rwx | g -> r-- | o -> ---

	WrongType error = fmt.Errorf("wrong type")
)

func BuildID(id string) []byte {
	key := make([]byte, highwayhash.Size)
	hash := highwayhash.Sum128([]byte(id), key)
	return []byte(hash[:])
}

func BuildIDAsString(id string) string {
	return fmt.Sprintf("%x", BuildID(id))
}
