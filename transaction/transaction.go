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
		TTL                  *TTL
	}
)

// NewOperation returns a new operation pointer
func NewOperation(colName string, content interface{}, key, val []byte, del, cleanHistory bool, ttl *TTL) *Operation {
	if del {
		content = nil
		val = nil
		ttl = nil
	}

	return &Operation{
		CollectionID: colName,
		Content:      content,

		DBKey:        key,
		Value:        val,
		Delete:       del,
		CleanHistory: cleanHistory,
		TTL:          ttl,
	}
}

// New builds a new write transaction struct with it's chanel
func New(ctx context.Context) *Transaction {
	return &Transaction{
		Ctx:          ctx,
		Operations:   []*Operation{},
		ResponseChan: make(chan error, 0),
	}
}

// NewWithOperation build a new transaction based on an Operation pointer
func NewWithOperation(ctx context.Context, op *Operation) *Transaction {
	return &Transaction{
		Ctx:          ctx,
		Operations:   []*Operation{op},
		ResponseChan: make(chan error, 0),
	}
}

// AddOperation add a new operation to the given transaction
func (t *Transaction) AddOperation(op *Operation) {
	t.Operations = append(t.Operations, op)
}

// GetWriteSize returns the length of bytes this transaction wants to write
func (t *Transaction) GetWriteSize() (ret int) {
	for _, op := range t.Operations {
		ret += len(op.Value)
	}
	return
}
