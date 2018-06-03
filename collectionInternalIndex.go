package gotinydb

import "fmt"

func (c *Collection) updateIndex(id string, index Index, newValue interface{}) error {
	// refs := c.getIndexReferences(id)
	//
	// if refs != nil {
	// 	// Clean old values
	// 	c.updateIndexAfterDelete(id, refs)
	// }
	//
	// // Build the reference
	// index.Put(newValue, id)
	// refs = append(refs, newIndexReference(index.getName(), newValue))
	//
	// return c.setIndexReferences(id, refs)

	fmt.Println("update index")
	return nil
}

func (c *Collection) updateIndexAfterDelete(id string, refs []*IndexReference) error {
	// if refs == nil {
	// 	refs, = c.getIndexReferences(id)
	// }
	//
	// for _, index := range c.Indexes {
	// 	for _, ref := range refs {
	// 		index.RemoveID(ref.GetValue(), id)
	// 	}
	// }

	fmt.Println("update index after delete")
	return nil
}
