package collection

import "fmt"

func (c *Collection) updateIndex(id string, newValue interface{}) error {
	file, refs, _ := c.loadIndexRefAndFile(id, true)
	defer file.Close()

	// Clean old values
	c.updateIndexAfterDelete(id, refs)

	newRefs := []*IndexReference{}
	for indexName, index := range c.Indexes {
		if newValue != nil {
			if value, apply := index.Apply(newValue); apply {
				index.Put(value, id)

				newRefs = append(newRefs, newIndexReference(indexName, value))
			}
		}
	}

	return c.setIndexReferenceWithFile(id, newRefs, file)
}

func (c *Collection) updateIndexAfterDelete(id string, refs []*IndexReference) error {
	if refs == nil {
		tmpRefs, getRefsErr := c.getIndexReference(id)
		if getRefsErr != nil {
			return fmt.Errorf("can't get the index references of %q: %s", id, getRefsErr.Error())
		}
		refs = tmpRefs
	}

	for _, ref := range refs {
		c.Indexes[ref.IndexName].RemoveID(ref.GetValue(), id)
	}
	return nil
}
