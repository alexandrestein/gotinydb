# GoTinyDB

[![GoDoc](https://godoc.org/github.com/alexandrestein/gotinydb?status.svg)](https://godoc.org/github.com/alexandrestein/gotinydb)
[![Travis Build](https://travis-ci.org/alexandrestein/gotinydb.svg?branch=master)](https://travis-ci.org/alexandrestein/gotinydb) 
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Falexandrestein%2Fgotinydb.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Falexandrestein%2Fgotinydb?ref=badge_shield)
[![AppVeyor Build](https://ci.appveyor.com/api/projects/status/7kr5y6kk9jtkg261?svg=true)](https://ci.appveyor.com/project/alexandrestein/gotinydb)
[![CircleCI](https://circleci.com/gh/alexandrestein/gotinydb.svg?style=svg)](https://circleci.com/gh/alexandrestein/gotinydb)
[![codecov](https://codecov.io/gh/alexandreStein/GoTinyDB/branch/master/graph/badge.svg)](https://codecov.io/gh/alexandreStein/GoTinyDB) 
[![Go Report Card](https://goreportcard.com/badge/github.com/alexandrestein/gotinydb)](https://goreportcard.com/report/github.com/alexandrestein/gotinydb)
[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

The goal is to have a fairly simple database which is light and don't needs to fit in RAM. It supports indexing for:

- string
- int, uint, int8, uint8, int16
- uint16, int32, uint32, int64, uint64
- time.Time

If the selector point to an array values inside the array are indexed.

## Installing

```bash
go get -u github.com/alexandrestein/gotinydb
```

## Getting started

The package is supposed to be used inside your software and at this point it is not supposed to be a dedicated database service.
Take a look at [GoDoc](https://godoc.org/github.com/alexandrestein/gotinydb) for examples.

## Built With

- [Badger](https://github.com/dgraph-io/badger) - Is the main storage engine
- [Btree](https://github.com/google/btree) - Is the in memory list used to save sub queries elements.
- [Structs](https://github.com/fatih/structs) - Used to cut objects in part for indexing

## Possible Road Map

- Make a basic master/slaves replication system for data protection
- Make a simple web interface
- Add "has" or "exist" filter
- Support float
- Full text search with [Bleve](http://www.blevesearch.com/)

## Contributing

Any contribution will be appreciate.
Feedbacks and suggestions are welcome.

## Vendoring

You can use [dep](https://github.com/golang/dep) or [vgo](https://github.com/golang/vgo/) for vendoring.

## Versioning

Version `v1.0.0` is not ready.
It's under development and versions have the form of v0.0.x.

There is no compatibility promise for now.

## Author

- **Alexandre Stein** - [GitHub](https://github.com/alexandrestein)

<!-- See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project. -->

## License

This project is licensed under the "Apache License, Version 2.0" see the [LICENSE](LICENSE) file for details or follow this [link](http://www.apache.org/licenses/LICENSE-2.0).

[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Falexandrestein%2Fgotinydb.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Falexandrestein%2Fgotinydb?ref=badge_large)

## Acknowledgments

- I was looking for pure `golang` database for reasonable (not to big) data set. I saw [Tiedot](https://github.com/HouzuoGuo/tiedot) long time ago but the index is only for exact match which was not what I was looking for.