package gotinydb

import "context"

type (
	writeTransaction struct {
		id               string
		contentInterface interface{}
		contentAsBytes   []byte
		responseChan     chan error
		ctx              context.Context
		bin              bool
		done             bool
	}
)

func newTransaction(id string) *writeTransaction {
	tr := new(writeTransaction)
	tr.id = id
	tr.responseChan = make(chan error, 0)

	return tr
}
