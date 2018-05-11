package collection

import "github.com/fatih/structs"

func (c *Collection) updateIndex(inputInterface interface{}, id string) error {
	input := structs.New(inputInterface).Map()

	for _, index := range c.Indexes {
		index.Update(input, id)
	}

	return nil
}

func (c *Collection) updateIndexAfterDelete(id string) error {
	for _, index := range c.Indexes {
		go index.RemoveId(id)
	}
	return nil
}
