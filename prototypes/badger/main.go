package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/dgraph-io/badger"
)

func main() {
	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// err = db.View(func(txn *badger.Txn) error {
	// 	_, err := txn.Get([]byte("key"))
	// 	// We expect ErrKeyNotFound
	// 	fmt.Println(err)
	// 	return nil
	// })

	// if err != nil {
	// 	log.Fatal(err)
	// }

	// txn := db.NewTransaction(true) // Read-write txn
	// err = txn.Set([]byte("key"), []byte("value"))
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// err = txn.Commit(nil)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	err = db.Update(func(txn *badger.Txn) error {
		err = txn.Set([]byte("key"), []byte("value"))
		if err != nil {
			log.Fatal(err)
		}
		return nil
	})

	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("key"))
		if err != nil {
			return err
		}
		val, err := item.Value()
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", string(val))
		return nil
	})

	if err != nil {
		fmt.Println("merde ici")
		log.Fatal(err)
	}
}
