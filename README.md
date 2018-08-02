# GoTinyDB

[![GoDoc](https://godoc.org/github.com/alexandrestein/gotinydb?status.svg)](https://godoc.org/github.com/alexandrestein/gotinydb)
[![Travis Build](https://travis-ci.org/alexandrestein/gotinydb.svg?branch=master)](https://travis-ci.org/alexandrestein/gotinydb) 
[![AppVeyor Build](https://ci.appveyor.com/api/projects/status/7kr5y6kk9jtkg261?svg=true)](https://ci.appveyor.com/project/alexandrestein/gotinydb)
[![CircleCI](https://circleci.com/gh/alexandrestein/gotinydb.svg?style=svg)](https://circleci.com/gh/alexandrestein/gotinydb)
[![codecov](https://codecov.io/gh/alexandreStein/GoTinyDB/branch/master/graph/badge.svg)](https://codecov.io/gh/alexandreStein/GoTinyDB) 
[![Go Report Card](https://goreportcard.com/badge/github.com/alexandrestein/gotinydb)](https://goreportcard.com/report/github.com/alexandrestein/gotinydb) 
[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

The goal is to have a fairly simple database which is light and don't needs to fit in RAM. It supports indexing for most of the basic Golang types.

## Installing

```bash
go get -u github.com/alexandrestein/gotinydb
```

## Getting started

The package is supposed to be used inside your software and at this point it is not supposed to be a dedicated database service.
Take a look at [GoDoc](https://godoc.org/github.com/alexandrestein/gotinydb) for examples.

## Built With

* [Badger](https://github.com/dgraph-io/badger) - Is the main storage engine
* [Bolt](https://github.com/boltdb/bolt) - Is the index engine
* [Btree](https://github.com/google/btree) - Is the in memory list used to save sub queries elements.
* [Structs](https://github.com/fatih/structs) - Used to cut objects in part for indexing

## Possible Road Map

* Make a backup service or replication system
* Add "has" or "exist" filter
* Make incremental backups
* Support float
* Full text search with [Bleve](http://www.blevesearch.com/)

## Contributing

Any contribution will be appreciate.
Feedbacks and suggestions are welcome.

## Vendoring

You can use [dep](https://github.com/golang/dep) or [vgo](https://github.com/golang/vgo/) for vendoring.

## Versioning

Version `v1.0.0` is now ready.
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
