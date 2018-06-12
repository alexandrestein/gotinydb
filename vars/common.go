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

// // ParseIDsBytesToIDsAsStrings takes a list of IDs as bytes and build a
// // list of strings.
// func ParseIDsBytesToIDsAsStrings(idsAsBytes []byte) (ids []string, err error) {
// 	err = json.Unmarshal(idsAsBytes, &ids)
// 	return
// }

// // FormatIDsStringsToIDsAsBytes takes a slice IDs as strings to build
// // a slice of bytes
// func FormatIDsStringsToIDsAsBytes(idsAsStrings []string) (ids []byte, err error) {
// 	return json.Marshal(idsAsStrings)
// }
