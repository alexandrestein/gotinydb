# GoTinyDB

[![GoDoc](https://godoc.org/github.com/alexandrestein/gotinydb?status.svg)](https://godoc.org/github.com/alexandrestein/gotinydb) [![Build Status](https://travis-ci.org/alexandreStein/GoTinyDB.svg?branch=master)](https://travis-ci.org/alexandreStein/GoTinyDB) [![codecov](https://codecov.io/gh/alexandreStein/GoTinyDB/branch/master/graph/badge.svg)](https://codecov.io/gh/alexandreStein/GoTinyDB) [![Go Report Card](https://goreportcard.com/badge/github.com/alexandrestein/gotinydb)](https://goreportcard.com/report/github.com/alexandrestein/gotinydb) [![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

The goal is to have a farly simple database wich is light and don't needs to fit in RAM. It supports indexing for most of the basic Golang types.

<!-- ## Installing

```bash
go get -u github.com/alexandrestein/gotinydb
```

## Getting Started

The package is supposed to be used inside your sofware and at this point it is not supose to be a "real" database service.

### Open database:
```golang
db, initErr := New(internalTesting.Path)
if initErr != nil {
  log.Fatal(initErr.Error())
  return
}
defer db.Close()
```

### Open collection:
```golang
col, colErr := db.Use("colectionName")
if colErr != nil {
  log.Fatal("openning test collection: %s", colErr.Error())
  return
}
```

### Setup an index for future queries:
```golang
// If you have user object like this:
// {UserName: string, Address: {Street: string, Num: int, City: string, ZIP: int}}
// and you want to index the username and the ZIP code.
if err := col.SetIndex(UserNameIndexName, utils.StringComparatorType, []string{"UserName"}); err != nil {
  log.Fatal(err)
}
if err := col.SetIndex(ZipIndexName, utils.IntComparatorType, []string{"Address","ZIP"}); err != nil {
  log.Fatal(err)
}
```
There is many types of index. Take a look at the [index documentation](https://godoc.org/github.com/alexandrestein/gotinydb/index).

### Put some data in the collection:
```golang
putErr := col.Put(objectID, objectOrBytes)
if putErr != nil {
  log.Error(putErr)
  return
}
```
The content can be an object or a stream of bytes. If it's a stream it needs to
have the form of `[]byte{}`.
This will adds and updates existing values.

### Get some data from the collection directly by it's the ID:
```golang
getErr := col.Get(objectID, receiver)
if getErr != nil {
  t.Error(getErr)
  return
}
```
The receiver can be an object ponter or a stream of bytes. If it's a stream it needs to
have the form of `*bytes.Buffer`.

### Get objects by query:
```golang
// Get IDs of object with ZIP code greater than 5000 less than 6000 and limited
// to 100 responses starting with elements with ZIP code are the closest from 5000
selector := []string{"Address","ZIP"}
limit := 100
q := NewQuery().SetLimit(limit)
// This defines the elements you want to retreive
wantAction := NewAction(Greater).SetSelector(selector).CompareTo(5000)
// This will removes ids from the list if match
doesNotWantAction := NewAction(Greater).SetSelector(selector).CompareTo(6000)

// Actualy do the query and get the IDs
ids := col.Query(q.Get(wantAction).Keep(doesNotWantAction))

// The first id will be 5000 if present and the rest will be orderd.
for i, id := range ids {
  fmt.Println(id)
}
```
This returns only a list of IDs. It's up to the caller to get the values he want
with the Get function. -->

## Built With

* [Badger](https://github.com/dgraph-io/badger) - Is the main storage engine
* [Bolt](https://github.com/boltdb/bolt) - Is the index engine
* [Structs](https://github.com/fatih/structs) - Used to cut objects in part for indexing

## To Do

* everything...

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Vendoring

We use [dep](https://github.com/golang/dep) or [vgo](https://github.com/golang/vgo/) for vendoring.

## Versioning

The package is under heavy developement for now and is just for testing and developement at this point.
Ones the design will be finalised the version will start at `1.0.0`.
For futur the versions, see the [tags on this repository](https://github.com/alexandrestein/gotinydb/tags).

## Authors

* **Alexandre Stein** - [GitHub](https://github.com/alexandrestein)

<!-- See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project. -->

## License

This project is licensed under the 3-Clause BSD License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments

* I was looking for pure `golang` database for reasonable (not to big) data size. I checked [Tiedot](https://github.com/HouzuoGuo/tiedot) long time ago but the index is only for exact match wich is not what I was looking for.
* B-Tree is a good way to have ordered elements and is extramly scalable.