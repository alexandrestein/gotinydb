package collection

func (c *Collection) updateIndex(oldValue, newValue interface{}, id string) error {
	for _, index := range c.Indexes {
		if newValue != nil {
			if value, apply := index.Apply(newValue); apply {
				index.Put(value, id)
			}
		}
		if oldValue != nil {
			if value, apply := index.Apply(oldValue); apply {
				index.RemoveID(value, id)
			}
		}
	}
	return nil
}
