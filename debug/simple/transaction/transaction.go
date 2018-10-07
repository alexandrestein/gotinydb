package transaction

import "context"

type (
	Transaction struct {
		Ctx context.Context

		DBKey, Value []byte
		Delete       bool

		ResponseChan chan error
	}
)

func NewTransaction(ctx context.Context, key, val []byte, del bool) *Transaction {
	responseChan := make(chan error, 0)
	return &Transaction{
		ctx,
		key, val,
		del,
		responseChan,
	}
}
