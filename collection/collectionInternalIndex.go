package collection

func (c *Collection) updateIndex(oldValue, newValue interface{}, id string) error {
	for _, index := range c.Indexes {
		go index.Update(oldValue, newValue, id)
	}

	return nil
}

func (c *Collection) updateIndexAfterDelete(id string) error {
	for _, index := range c.Indexes {
		go index.RemoveId(id)
	}
	return nil
}
