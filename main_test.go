package gotinydb

import (
	"crypto/sha1"
	"fmt"
	"os"
	"time"
)

var (
	getTestPathChan chan string
)

func init() {
	getTestPathChan = make(chan string)
	buf, _ := time.Now().MarshalBinary()
	randBytes := sha1.Sum(buf)
	randPart := fmt.Sprintf("%x", randBytes[:4])
	nTest := 0
	go func() {
		for {
			path := fmt.Sprintf("%s/gotinydb-%s-%d", os.TempDir(), randPart, nTest)
			getTestPathChan <- path
			nTest++
		}
	}()
}
