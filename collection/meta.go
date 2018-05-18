package collection

import "gitea.interlab-net.com/alexandre/db/index"

type (
	// IndexMeta defines on drive the stat of the indexes
	IndexMeta struct {
		Name     string
		Selector []string
		Type     index.Type
	}
)

func NewMeta(name string, selector []string, t index.Type) *IndexMeta {
	return &IndexMeta{name, selector, t}
}
