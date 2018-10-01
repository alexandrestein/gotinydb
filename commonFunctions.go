package gotinydb

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"io/ioutil"

	"golang.org/x/crypto/blake2b"
)

func getIDsAsString(input []*idType) (ret []string) {
	for _, id := range input {
		ret = append(ret, id.ID)
	}
	return ret
}

func newTransactionElement(id string, content interface{}, isInsertion bool, col *Collection) (wtElem *writeTransactionElement) {
	wtElem = &writeTransactionElement{
		id: id, contentInterface: content, isInsertion: isInsertion, collection: col,
	}

	if !isInsertion {
		return
	}

	if bytes, ok := content.([]byte); ok {
		wtElem.bin = true
		wtElem.contentAsBytes = bytes
	}

	if !wtElem.bin {
		jsonBytes, marshalErr := json.Marshal(content)
		if marshalErr != nil {
			return nil
		}

		wtElem.contentAsBytes = jsonBytes
	}

	return
}

func newFileTransactionElement(id string, chunkN int, content []byte, isInsertion bool) *writeTransactionElement {
	return &writeTransactionElement{
		id: id, chunkN: chunkN, contentAsBytes: content, isInsertion: isInsertion, isFile: true,
	}
}

func newTransaction(ctx context.Context) *writeTransaction {
	wt := new(writeTransaction)
	wt.ctx = ctx
	wt.responseChan = make(chan error, 0)

	return wt
}

func (wt *writeTransaction) addTransaction(trElement ...*writeTransactionElement) {
	wt.transactions = append(wt.transactions, trElement...)
}

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

func indexZiper(baseFolder string) ([]byte, error) {
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

			// Recurse
			newBase := basePath + file.Name() + "/"

			err := addFiles(w, newBase, file.Name()+"/")
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func indexDeziper(baseFolder string, contentAsZip []byte) error {
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
