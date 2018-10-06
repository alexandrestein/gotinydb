package simple

import "github.com/blevesearch/bleve"

type (
	BleveIndex struct {
		dbElement

		collection *Collection

		BleveIndex bleve.Index `json:"-"`
		Selector   selector
		Path       string
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