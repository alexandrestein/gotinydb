package gotinydb

func (c *Collection) updateIndex(id string, index Index, newValue interface{}) error {
	refs := c.getIndexReferences(id)

	if refs != nil {
		// Clean old values
		c.updateIndexAfterDelete(id, refs)
	}

	// Build the reference
	index.Put(newValue, id)
	refs = append(refs, newIndexReference(index.getName(), newValue))

	return c.setIndexReferences(id, refs)
}

func (c *Collection) updateIndexAfterDelete(id string, refs []*IndexReference) error {
	if refs == nil {
		refs = c.getIndexReferences(id)
	}

	for _, ref := range refs {
		c.Indexes[ref.IndexName].RemoveID(ref.GetValue(), id)
	}
	return nil
}
