package archive

import (
	"archive/zip"
	"encoding/json"
	"os"
	"time"
)

type (
	// Archive defines the way archives are saved inside the zip file
	Archive struct {
		FirstBackup, LastBackup time.Time
		Recorders               []uint64
		Indexes                 []*Index

		file *os.File
	}

	// Index defines the indexes
	Index struct {
		Name     string
		Selector []string
		Type     int
	}
)

func NewArchive(file *os.File) (*Archive, error) {
	// Open a zip archive for reading.
	r, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	ret := new(Archive)

	// Iterate through the files in the archive,
	// printing some of their contents.
	for _, f := range r.File {
		if f.Name == "archive.json" {
			fileAsReader, openFileErr := f.Open()
			if openFileErr != nil {
				return nil, openFileErr
			}

			buf := make([]byte, 1000*100)
			n, readErr := fileAsReader.Read(buf)
			if readErr != nil {
				return nil, readErr
			} else {
				buf = buf[:n]
			}

			unmarshalErr := json.Unmarshal(buf, ret)
			if unmarshalErr != nil {
				return nil, unmarshalErr
			}

			ret.file = file
			return ret, nil
		}
	}

	return ret, nil
}

func (a *Archive) AddBackup() {

}
