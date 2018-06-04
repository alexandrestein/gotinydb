package gotinydb

import "github.com/alexandrestein/gotinydb/vars"

type (
	Index struct {
		Name     string
		Selector []string
		Type     vars.IndexType
	}
)
