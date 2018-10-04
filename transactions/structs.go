package transactions

import (
	"context"
)

type (
	WriteTransaction struct {
		ResponseChan chan error
		Ctx          context.Context
		Transactions []*WriteElement
	}

	WriteElement struct {
		// id                  string
		// collection          *Collection
		// contentInterface    interface{}
		// chunkN              int
		// bin                 bool
		// isInsertion, isFile bool
		// bleveIndex          bool

		DBKey          []byte
		ContentAsBytes []byte
	}
)

func NewTransactionElement(dbKey, content []byte) *WriteElement {
	return &WriteElement{
		DBKey: dbKey, ContentAsBytes: content,
	}
}

func NewTransaction(ctx context.Context) *WriteTransaction {
	if ctx == nil {
		return nil
	}

	wt := new(WriteTransaction)
	wt.Ctx = ctx
	wt.ResponseChan = make(chan error, 0)

	return wt
}

func (wt *WriteTransaction) AddTransaction(trElement ...*WriteElement) {
	wt.Transactions = append(wt.Transactions, trElement...)
}
