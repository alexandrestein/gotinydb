package gotinydb

import (
	"github.com/blevesearch/bleve"
)

func (i *bleveIndex) open() error {
	if i.index != nil {
		return nil
	}

	bleveIndex, err := bleve.OpenUsing(i.Path, i.kvConfig)
	if err != nil {
		return err
	}

	i.index = bleveIndex

	return nil
}
