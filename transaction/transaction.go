package transaction

import (
	"context"
)

type (
	// Transaction defines the struct to manage sequential writes
	Transaction struct {
		Ctx context.Context

		Operations []*Operation

		ResponseChan chan error
	}

	// Operation defines the real writes to be done
	Operation struct {
		CollectionID string
		Content      interface{}

		DBKey, Value         []byte
		Delete, CleanHistory bool
	}
)

// NewOperation returns a new operation pointer
func NewOperation(id string, content interface{}, key, val []byte, del bool) *Operation {
	return &Operation{
		CollectionID: id,
		Content:      content,

		DBKey:  key,
		Value:  val,
		Delete: del, CleanHistory: false,
	}
}

// New builds a new write transaction struct with it's chanel
func New(ctx context.Context, id string, content interface{}, key, val []byte, del bool) *Transaction {
	if del {
		val = nil
	}

	ope := NewOperation(id, content, key, val, del)

	return &Transaction{
		Ctx:          ctx,
		Operations:   []*Operation{ope},
		ResponseChan: make(chan error, 0),
	}
}

// AddOperation add a new operation to the given transaction
func (t *Transaction) AddOperation(id string, content interface{}, key, val []byte, del bool) {
	ope := NewOperation(id, content, key, val, del)

	t.Operations = append(t.Operations, ope)
}

// GetWriteSize returns the length of bytes this transaction wants to write
func (t *Transaction) GetWriteSize() (ret int) {
	for _, op := range t.Operations {
		ret += len(op.Value)
	}
	return
}
