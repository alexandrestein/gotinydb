# GoTinyDB

[![GoDoc](https://godoc.org/github.com/alexandreStein/GoTinyDB?status.svg)](https://godoc.org/github.com/alexandreStein/GoTinyDB) [![Build Status](https://travis-ci.org/alexandreStein/GoTinyDB.svg?branch=master)](https://travis-ci.org/alexandreStein/GoTinyDB)[![Go Report Card](https://goreportcard.com/badge/github.com/alexandreStein/GoTinyDB)](https://goreportcard.com/report/github.com/alexandreStein/GoTinyDB) [![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

The goal is to have a farly simple database wich is light and don't needs to fit
in RAM. It supports indexing for most of the basic Golang types.

### Installing

```bash
go get -u github.com/alexandreStein/GoTinyDB
```

## Getting Started

The package is supposed to be used inside your sofware and at this point it is not supose to be a real database server.

In your package you will init the database with something like:
```golang
db, initErr := New(internalTesting.Path)
if initErr != nil {
  log.Fatal(initErr.Error())
  return
}
```

Ones init you will get or build a collection:
```golang
col, colErr := db.Use("colectionName")
if colErr != nil {
  log.Fatal("openning test collection: %s", colErr.Error())
  return
}
```

Then the operations are done directly on the collection ponter:
To have access to the query you need to build the index before puting data.
```golang
if err := col.SetIndex(UserNameIndexName, utils.StringComparatorType, []string{"UserName"}); err != nil {
  log.Fatal(err)
}
if err := col.SetIndex(ZipIndexName, utils.IntComparatorType, []string{"Address","ZIP"}); err != nil {
  log.Fatal(err)
}
```
There is many types of index. Take a look at the [index documentation](indexDOC).

Put some data in the collection:
```golang
putErr := col.Put(objectID, content)
if putErr != nil {
  log.Error(putErr)
  return
}
```
The content can be an object or a stream of bytes. If it's a stream it needs to
have the form of `[]byte{}`.
This will adds and updates existing values.

To get some data from the collection directly from the ID:
```golang
getErr := col.Get(objectID, receiver)
if getErr != nil {
  t.Error(getErr)
  return
}
```
The receiver can be an object or a stream of bytes. If it's a stream it needs to
have the form of `*bytes.Buffer`.

When you need to get value based on the sub elements use the query function with:
```golang
selector := []string{"Address","ZIP"}
limit := 100
q := query.NewQuery(selector).SetLimit(limit)
// This defines the elements you want to retreive
wantAction := query.NewAction(query.Greater).CompareTo(5000)
// This will removes ids from the list if it match this action
doesNotWantAction := query.NewAction(query.Greater).CompareTo(6000)

// Actualy do the query and get the IDs
ids := col.Query(q.Get(wantAction).Keep(doesNotWantAction))

// The first id will be 5000 if present and the rest will be orderd.
for i, id := range ids {
  fmt.Println(id)
}
```
This returns only a list of IDs. It's up to the caller to get the values he want
with the Get function.

## Built With

* [Gods](https://github.com/emirpasic/gods) - The B-Tree implementation (redirected with `dep` or `vgo` because pull request not accepted yet)
* [Structs](https://github.com/fatih/structs) - The package used to cut objects in
part for indexing

## To Do

* Build write serialiser at the object level (at this point there is no concurancy protection)

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Vendoring

We use [dep](https://github.com/golang/dep) or [vgo](https://github.com/golang/vgo/) for vendoring.

## Versioning

The package is under heavy developement for now and is just for testing and developement at this point.
Ones the design will be finalised the version will start at `1.0.0`.

For futur the versions, see the [tags on this repository](https://github.com/alexandreStein/GoTinyDB/tags).

## Authors

* **Alexandre Stein** - [GitHub](https://github.com/alexandreStein)

<!-- See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project. -->

## License

This project is licensed under the 3-Clause BSD License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments

* I was looking for pure `golang` database for reasonable (not to big) data size. I checked [Tiedot](https://github.com/HouzuoGuo/tiedot) long time ago but the index is only for exact match wich is not what I was looking for.
* B-Tree is a good way to have ordered elements and is extramly scalable.
