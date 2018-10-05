package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"time"

	"github.com/alexandrestein/gotinydb/blevestore"
	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/transactions"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/index/upsidedown"
	"github.com/dgraph-io/badger"
)

var (
	key        = [32]byte{}
	db         *badger.DB
	writesChan = make(chan *transactions.WriteTransaction, 0)

	prefix = []byte{1, 0, 1, 19, 132, 19, 146, 76, 219, 231, 150}
)

// var (
// 	key = [32]byte{}

// 	encryptFunc = func(dbID, clearContent []byte) (encryptedContent []byte) {
// 		return cipher.Encrypt(key, dbID, clearContent)
// 	}
// 	decryptFunc = func(dbID, encryptedContent []byte) (clearContent []byte, _ error) {
// 		return cipher.Decrypt(key, dbID, encryptedContent)
// 	}
// )

func init() {
	rand.Read(key[:])

	go goRoutineLoopForWrites()
}

func goRoutineLoopForWrites() {
	for {
		ops, ok := <-writesChan
		if !ok {
			return
		}

		err := db.Update(func(txn *badger.Txn) error {
			for _, op := range ops.Transactions {
				var err error
				if op.ContentAsBytes == nil {
					// if op.ContentAsBytes == nil || len(op.ContentAsBytes) == 0 {
					err = txn.Delete(op.DBKey)
				} else {
					err = txn.Set(op.DBKey, cipher.Encrypt(key, op.DBKey, op.ContentAsBytes))
				}
				if err != nil {
					fmt.Println(err)
				}
			}
			return nil
		})
		ops.ResponseChan <- err

		if err != nil {
			fmt.Println(err)
		}
	}
}

func main() {
	defer os.RemoveAll("email")
	os.RemoveAll("email")

	dbDir := "dbDir"
	defer os.RemoveAll(dbDir)
	os.RemoveAll(dbDir)

	options := badger.DefaultOptions
	options.Dir = dbDir
	options.ValueDir = dbDir

	var err error
	db, err = badger.Open(options)
	if err != nil {
		fmt.Println("err", err)
		return
	}

	config := blevestore.NewBleveStoreConfigMap("email", key, prefix, db, writesChan, time.Second*60)
	index, err := bleve.NewUsing("email", bleve.NewIndexMapping(), upsidedown.Name, blevestore.Name, config)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = index.Index("1_16", "dijkstra-17@artaxerxes.com")
	if err != nil {
		fmt.Println("err", err)
		return
	}

	query := bleve.NewQueryStringQuery("dijkstra-17@artaxerxes.com")
	searchRequest := bleve.NewSearchRequestOptions(query, 10, 0, true)
	searchResult, _ := index.Search(searchRequest)

	fmt.Println(searchResult)

	index.Close()
	db.Close()

	fmt.Println("close...")
	fmt.Println()
	fmt.Println()
	fmt.Println()

	db, err = badger.Open(options)
	if err != nil {
		fmt.Println("err", err)
		return
	}

	config = blevestore.NewBleveStoreConfigMap("email", key, prefix, db, writesChan, time.Second*60)

	index, _ = bleve.OpenUsing("email", config)

	query = bleve.NewQueryStringQuery("dijkstra-17@artaxerxes.com")
	searchRequest = bleve.NewSearchRequestOptions(query, 10, 0, true)
	searchResult, _ = index.Search(searchRequest)

	fmt.Println(searchResult)

	index.Close()
	db.Close()

	fmt.Println("close...")
	fmt.Println()
	fmt.Println()
	fmt.Println()

	db, err = badger.Open(options)
	if err != nil {
		fmt.Println("err", err)
		return
	}

	config = blevestore.NewBleveStoreConfigMap("email", key, prefix, db, writesChan, time.Second*60)

	index, _ = bleve.OpenUsing("email", config)

	query = bleve.NewQueryStringQuery("dijkstra-17@artaxerxes.com")
	searchRequest = bleve.NewSearchRequestOptions(query, 10, 0, true)
	searchResult, _ = index.Search(searchRequest)

	fmt.Println(searchResult)
}
