package transaction

type (
	Transaction struct {
		DBKey, Value []byte
		Delete       bool

		ResponseChan chan error
	}
)

func NewTransaction(key, val []byte, del bool) *Transaction {
	responseChan := make(chan error, 0)
	return &Transaction{
		key, val,
		del,
		responseChan,
	}
}
