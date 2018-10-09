package transaction

import "context"

type (
	Transaction struct {
		Ctx context.Context

		DBKey, Value         []byte
		Delete, CleanHistory bool

		ResponseChan chan error
	}
)

func NewTransaction(ctx context.Context, key, val []byte, del bool) *Transaction {
	responseChan := make(chan error, 0)
	return &Transaction{
		Ctx:   ctx,
		DBKey: key, Value: val,
		Delete: del, CleanHistory: false,
		ResponseChan: responseChan,
	}
}
