package gotinydb

import (
	"fmt"

	"github.com/alexandrestein/gotinydb/vars"
)

func (c *Collection) buildStoreID(id string) []byte {
	compositeID := fmt.Sprintf("%s_%s", c.Name, id)
	return vars.BuildID(compositeID)
}
