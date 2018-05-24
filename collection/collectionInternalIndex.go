package collection

import "fmt"

func (c *Collection) updateIndex(oldValue, newValue interface{}, id string) error {
	fmt.Println("ID and value", id, oldValue, newValue)
	for _, index := range c.Indexes {
		if newValue != nil {
			if value, apply := index.Apply(newValue); apply {
				index.Put(value, id)
			}
		}
		if oldValue != nil {
			if value, apply := index.Apply(oldValue); apply {
				fmt.Println("index.GetAllIndexedValues()", index.GetAllIndexedValues())
				index.RemoveID(value, id)
				fmt.Println("index.GetAllIndexedValues()", index.GetAllIndexedValues())
			} else {
				fmt.Println("do not apply", oldValue, apply, index.GetSelector())
			}
		}
	}
	return nil
}
