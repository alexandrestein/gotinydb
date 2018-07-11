# GoTinyDB

[![GoDoc](https://godoc.org/github.com/alexandrestein/gotinydb?status.svg)](https://godoc.org/github.com/alexandrestein/gotinydb) [![Build Status](https://travis-ci.org/alexandrestein/gotinydb.svg?branch=master)](https://travis-ci.org/alexandrestein/gotinydb) [![codecov](https://codecov.io/gh/alexandreStein/GoTinyDB/branch/master/graph/badge.svg)](https://codecov.io/gh/alexandreStein/GoTinyDB) [![Go Report Card](https://goreportcard.com/badge/github.com/alexandrestein/gotinydb)](https://goreportcard.com/report/github.com/alexandrestein/gotinydb) [![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

The goal is to have a fairly simple database which is light and don't needs to fit in RAM. It supports indexing for most of the basic Golang types.

## Installing

```bash
go get -u github.com/alexandrestein/gotinydb
```

## Getting started

The package is supposed to be used inside your software and at this point it is not supposed to be a dedicated database service.
Take a look at [GoDoc](https://godoc.org/github.com/alexandrestein/gotinydb) for examples.

It's hard to have accurate documentation when they are not automatically build from source.
That why I prefer to put all documentation inside [GoDoc](https://godoc.org/github.com/alexandrestein/gotinydb)

## Built With

* [Badger](https://github.com/dgraph-io/badger) - Is the main storage engine
* [Bolt](https://github.com/boltdb/bolt) - Is the index engine
* [Btree](https://github.com/google/btree) - Is the in memory list used to save sub queries elements.
* [Structs](https://github.com/fatih/structs) - Used to cut objects in part for indexing

## Road Map

### v1.0.0

* Make a backup function
* Add checksum
* Make some benchmark

### v1.X.0

* Add "has" or "exist" filter
* Support float if asked
* Full text search with [Bleve](http://www.blevesearch.com/)

## Contributing

Any contribution will be appreciate.
Feedbacks and suggestions are at this point very very important to me.

## Vendoring

You can use [dep](https://github.com/golang/dep) or [vgo](https://github.com/golang/vgo/) for vendoring.

## Versioning

The package is under heavy development for now and is just for testing and development at this point.
Ones the design will be finalized the version will start at `v1.0.0`.
For future the versions, see the [tags on this repository](https://github.com/alexandrestein/gotinydb/tags).

## Author

* **Alexandre Stein** - [GitHub](https://github.com/alexandrestein)

<!-- See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project. -->

## License

This project is licensed under the "Apache License, Version 2.0" - see the [LICENSE](LICENSE) file for details or follow this [link](http://www.apache.org/licenses/LICENSE-2.0).

## Acknowledgments

* I was looking for pure `golang` database for reasonable (not to big) data set. I saw [Tiedot](https://github.com/HouzuoGuo/tiedot) long time ago but the index is only for exact match which was not what I was looking for.
* B-Tree is a good way to have ordered elements and is extremely scalable. It is particularly great for heavy random reads. Used for indexing and queries ([Bolt](https://github.com/boltdb/bolt) for persistent indexation and [Btree](https://github.com/google/btree) for in memory queries).
* LSM-Tree is much more efficient for height write loads. This is used to store data ([Badger](https://github.com/dgraph-io/badger)).
