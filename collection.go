package db

func NewCollection(path string) *Collection {
	c := new(Collection)
	c.path = path

	return c
}

func (c *Collection) SetIndex(target string) error {

	return nil
}
