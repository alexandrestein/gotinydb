package GoTinyDB

import "fmt"

func (c *Collection) updateIndex(id string, newValue interface{}) error {
	refs, getIndexRefErr := c.getIndexReferences(id)
	if getIndexRefErr != nil {
		return fmt.Errorf("getting the index references: %s", getIndexRefErr.Error())
	}

	// Clean old values
	c.updateIndexAfterDelete(id, refs)

	newRefs := []*IndexReference{}
	for indexName, index := range c.Indexes {
		if newValue != nil {
			index.Put(newValue, id)
			newRefs = append(newRefs, newIndexReference(indexName, newValue))
		}
	}

	return c.setIndexReferences(id, newRefs)
}

func (c *Collection) updateIndexAfterDelete(id string, refs []*IndexReference) error {
	if refs == nil {
		tmpRefs, getRefsErr := c.getIndexReferences(id)
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
