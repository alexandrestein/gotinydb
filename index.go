package gotinydb

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"io"
	"io/ioutil"

	"github.com/blevesearch/bleve"
)

type (
	BleveIndex struct {
		dbElement

		collection *Collection

		BleveIndex bleve.Index `json:"-"`
		Selector   selector
		Path       string

		BleveIndexAsBytes []byte
	}
)

func NewIndex(name string) *BleveIndex {
	return &BleveIndex{
		dbElement: dbElement{
			Name: name,
		},
	}
}

func (i *BleveIndex) Close() error {
	return i.BleveIndex.Close()
}

func (i *BleveIndex) indexZipper() ([]byte, error) {
	// Get a Buffer to Write To
	buff := bytes.NewBuffer(nil)

	// Create a new zip archive.
	w := zip.NewWriter(buff)
	w.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(out, flate.BestCompression)
	})

	// Add some files to the archive.
	err := i.addFiles(w, i.Path, "")
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

func (i *BleveIndex) addFiles(w *zip.Writer, basePath, baseInZip string) error {
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

			err := i.addFiles(w, newBase, file.Name()+"/")
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (i *BleveIndex) indexUnzipper() error {
	buff := bytes.NewReader(i.BleveIndexAsBytes)
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

		err = ioutil.WriteFile(i.Path+"/"+f.Name, fileBytes, 0640)
		if err != nil {
			return err
		}
		rc.Close()
	}

	return nil
}
