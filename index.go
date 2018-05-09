package db

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/emirpasic/gods/trees/btree"
)

func NewIndex(path string) *Index {
	b := new(Index)
	b.tree = btree.NewWithStringComparator(10)
	b.path = path

	return b
}

func (b *Index) Save() error {
	treeAsBytes, jsonErr := b.tree.ToJSON()
	if jsonErr != nil {
		return fmt.Errorf("durring JSON convertion: %s", jsonErr.Error())
	}

	file, fileErr := os.OpenFile(b.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePermission)
	if fileErr != nil {
		return fmt.Errorf("opening file: %s", fileErr.Error())
	}

	n, writeErr := file.WriteAt(treeAsBytes, 0)
	if writeErr != nil {
		return fmt.Errorf("writing file: %s", writeErr.Error())
	}
	if n != len(treeAsBytes) {
		return fmt.Errorf("writes no complet, writen %d and have %d", len(treeAsBytes), n)
	}

	return nil
}

func (b *Index) Load() error {
	file, fileErr := os.OpenFile(b.path, os.O_RDONLY, filePermission)
	if fileErr != nil {
		return fmt.Errorf("opening file: %s", fileErr.Error())
	}

	buf := bytes.NewBuffer(nil)
	at := int64(0)
	for {
		tmpBuf := make([]byte, blockSize)
		n, readErr := file.ReadAt(tmpBuf, at)
		if readErr != nil {
			if io.EOF == readErr {
				buf.Write(tmpBuf[:n])
				break
			}
			return fmt.Errorf("%d readed but: %s", n, readErr.Error())
		}
		at = at + int64(n)
		buf.Write(tmpBuf)
	}

	err := b.tree.FromJSON(buf.Bytes())
	if err != nil {
		return fmt.Errorf("parsing block: %s", err.Error())
	}

	return nil
}
