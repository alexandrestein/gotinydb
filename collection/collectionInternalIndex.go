package collection

func (c *Collection) updateIndex(oldValue, newValue interface{}, id string) error {
	for _, index := range c.Indexes {
		if value, apply := index.Apply(newValue); apply {
			index.Put(value, id)
		}
		if oldValue != nil {
			if value, apply := index.Apply(oldValue); apply {
				index.RemoveID(value, id)
			}
		}
	}
	return nil
}

func (c *Collection) updateIndexAfterDelete(id string) error {
	// for _, index := range c.Indexes {
	// 	go index.RemoveId(id)
	// }
	return nil
}
