package gotinydb

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"encoding/binary"
	"io"
	"io/ioutil"

	"github.com/dgraph-io/badger"
	"golang.org/x/crypto/blake2b"
)

func getIDsAsString(input []*idType) (ret []string) {
	for _, id := range input {
		ret = append(ret, id.ID)
	}
	return ret
}

// func newTransactionElement(id string, content interface{}, isInsertion bool, col *Collection) (wtElem *transactions.WriteTransaction) {
// 	fmt.Println("newTransactionElement")
// 	return nil

// 	// wtElem = &transactions.WriteElement{
// 	// 	id: id, contentInterface: content, isInsertion: isInsertion, collection: col,
// 	// }

// 	// if !isInsertion {
// 	// 	return
// 	// }

// 	// if bytes, ok := content.([]byte); ok {
// 	// 	wtElem.bin = true
// 	// 	wtElem.contentAsBytes = bytes
// 	// }

// 	// if !wtElem.bin {
// 	// 	jsonBytes, marshalErr := json.Marshal(content)
// 	// 	if marshalErr != nil {
// 	// 		return nil
// 	// 	}

// 	// 	wtElem.contentAsBytes = jsonBytes
// 	// }

// 	// return
// }

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

func indexZipper(baseFolder string) ([]byte, error) {
	// Get a Buffer to Write To
	buff := bytes.NewBuffer(nil)
	// outFile, err := os.Create(`/Users/tom/Desktop/zip.zip`)
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// defer outFile.Close()

	// Create a new zip archive.
	w := zip.NewWriter(buff)
	w.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(out, flate.BestCompression)
	})

	// Add some files to the archive.
	err := addFiles(w, baseFolder, "")
	if err != nil {
		return nil, err
	}

	// Make sure to check the error on Close.
	err = w.Close()
	if err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

func addFiles(w *zip.Writer, basePath, baseInZip string) error {
	// Open the Directory
	files, err := ioutil.ReadDir(basePath)
	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.IsDir() {
			dat, err := ioutil.ReadFile(basePath + "/" + file.Name())
			if err != nil {
				return err
			}

			// Add some files to the archive.
			f, err := w.Create(baseInZip + file.Name())
			if err != nil {
				return err
			}
			_, err = f.Write(dat)
			if err != nil {
				return err
			}
		} else if file.IsDir() {

			newBase := basePath + file.Name() + "/"

			err := addFiles(w, newBase, file.Name()+"/")
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func indexUnzipper(baseFolder string, contentAsZip []byte) error {
	buff := bytes.NewReader(contentAsZip)
	// Open a zip archive for reading.
	r, err := zip.NewReader(buff, int64(buff.Len()))
	if err != nil {
		return err
	}
	r.RegisterDecompressor(zip.Deflate, func(r io.Reader) io.ReadCloser {
		return flate.NewReader(r)
	})

	// Iterate through the files in the archive,
	// printing some of their contents.
	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			return err
		}

		var fileBytes []byte
		fileBytes, err = ioutil.ReadAll(rc)
		if err != nil {
			return err
		}

		err = ioutil.WriteFile(baseFolder+"/"+f.Name, fileBytes, 0640)
		if err != nil {
			return err
		}
		rc.Close()
	}

	return nil
}

// deriveName returns a hash of the given name with the requested size.
// The size can't be greater than 32 or zero.
// If bigger than 32, the returned slice is 32 bytes long and if zero or less
// it returns a 8 bytes slice
func deriveName(name []byte, size int) []byte {
	if size > 32 {
		size = 32
	} else if size <= 0 {
		size = 8
	}

	bs := blake2b.Sum256(name)
	return bs[:size]
}

func deleteLoop(db *badger.DB, prefix []byte) (done bool, err error) {
	return done, db.Update(func(txn *badger.Txn) error {
		done, err = deleteLoopViaTxn(txn, prefix)
		return err
	})
}

func deleteLoopViaTxn(txn *badger.Txn, prefix []byte) (done bool, _ error) {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchValues = false
	it := txn.NewIterator(opt)
	defer it.Close()

	counter := 1

	// Remove the index DB files
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		err := txn.Delete(it.Item().Key())
		if err != nil {
			return false, err
		}

		if counter%10000 == 0 {
			return false, nil
		}

		counter++
	}

	return true, nil
}
