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

// // CompareBytes is semilar to bytes.Compare but count differently.
// // It loop every element of the slice until one is bigger than the other.
// // If sliceA smaller it returns -1, if sliceA is bigger it returns 1.
// // And zero if equal
// func CompareBytes(sliceA, sliceB []byte) int {
// 	var maxLen int
// 	if len(sliceA) > len(sliceB) {
// 		maxLen = len(sliceA)
// 	} else {
// 		maxLen = len(sliceB)
// 	}
// 	for i := maxLen - 1; i >= 0; i-- {
// 		a := sliceA[i]
// 		b := sliceB[i]

// 		fmt.Println("a b", a, b)
// 		if a > b {
// 			return 1
// 		} else if a < b {
// 			return -1
// 		} else if a == b {
// 			continue
// 		}
// 	}
// 	// for j, a := range sliceA {
// 	// 	b := sliceB[j]
// 	// 	if a > b {
// 	// 		return 1
// 	// 	} else if a < b {
// 	// 		return -1
// 	// 	} else if a == b {
// 	// 		continue
// 	// 	}
// 	// }
// 	return 0
// }
