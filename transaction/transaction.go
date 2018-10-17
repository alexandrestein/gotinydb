package transaction

import "context"

type (
	// Transaction defines the struct to manage sequential writes
	Transaction struct {
		Ctx context.Context

		DBKey, Value         []byte
		Delete, CleanHistory bool

		ResponseChan chan error
	}
)

// NewTransaction builds a new write transaction struct with it's chanel
func NewTransaction(ctx context.Context, key, val []byte, del bool) *Transaction {
	if del {
		val = nil
	}

	return &Transaction{
		Ctx:   ctx,
		DBKey: key, Value: val,
		Delete: del, CleanHistory: false,
		ResponseChan: make(chan error, 0),
	}
}
