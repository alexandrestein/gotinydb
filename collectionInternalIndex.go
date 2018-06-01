package gotinydb

func (c *Collection) updateIndex(id string, index Index, newValue interface{}) error {
	refs := c.getIndexReferences(id)

	// Clean old values
	c.updateIndexAfterDelete(id, refs)

	// Build the reference
	newRefs := []*IndexReference{}
	index.Put(newValue, id)
	newRefs = append(newRefs, newIndexReference(index.getName(), newValue))

	return c.setIndexReferences(id, newRefs)
}

func (c *Collection) updateIndexAfterDelete(id string, refs []*IndexReference) error {
	if refs == nil {
		tmpRefs := c.getIndexReferences(id)
		refs = tmpRefs
	}

	for _, ref := range refs {
		c.Indexes[ref.IndexName].RemoveID(ref.GetValue(), id)
	}
	return nil
}
