# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/alexandrestein/gotinydb/compare/v0.6.1...master)

## [0.6.2](https://github.com/alexandrestein/gotinydb/compare/v0.6.1...v0.6.2)

### Add 

- Timeout for file reader and writer.

### Changed

- Limit file size of the Badger database. The goal is to take advantage of the garbage collection which needs multiple files to work.
- CI now run test twice. Ones with verbose parameter and the second with race. This should run every test function and after eventually with the race condition.
- Upgrade Badger from short after v1.5.3 to v2.0.0.rc-2.

### Fixes

- The management path introduced in [commit](https://github.com/alexandrestein/gotinydb/commit/8a3c019c3dd92986564b0415f1e27bcb4ba7ab29) was not working for backup and restore.

## [0.6.1](https://github.com/alexandrestein/gotinydb/compare/v0.6.0...v0.6.1)

### Add 

- New command to run database garbage collection. ([commit](https://github.com/alexandrestein/gotinydb/commit/24bf0d85293ddefcad8ef47e7ec8da262fd5f8ff))

## [0.6.0](https://github.com/alexandrestein/gotinydb/compare/v0.5.1...v0.6.0)

### Fixes

- **BREAKING CHANGES**: If the database was open from on location and then from an other one. The path of the index was lost. ([commit](https://github.com/alexandrestein/gotinydb/commit/8a3c019c3dd92986564b0415f1e27bcb4ba7ab29))
- **BREAKING CHANGES**: Tells **Bleve** to not save values just index it. ([commit](https://github.com/alexandrestein/gotinydb/commit/85c5ee3fe7b0c52b0c0f6926cd3791868cd5bc84))

## [0.5.1](https://github.com/alexandrestein/gotinydb/compare/v0.5.0...v0.5.1)

### Fixes

- Bad file length after write. ([commit](https://github.com/alexandrestein/gotinydb/commit/404723961306176253538b5b19aa5aad54839461))

## [0.5.0](https://github.com/alexandrestein/gotinydb/compare/v0.4.1...v0.5.0)

### Changed

- Externalized the file storage system from the main DB object. Now all files actions are done in an other object called FileStore. The goal is to make the package easier to understand and use.
- Tests are not running race test on TestBackup and TestMain.

## [0.4.1](https://github.com/alexandrestein/gotinydb/compare/v0.4.0...v0.4.1)

### Add

- Add TTL (Time To Live) support for files and documents.

### Changed

- Garbage collection is done every 15 minutes instead of every 12 hours.

### Fixes

- When try to open file which is deleted it returns an error and not an empty file.

## [0.4.0](https://github.com/alexandrestein/gotinydb/compare/v0.3.3...v0.4.0)

### Add

- Add cache to file reader to prevent unnecessary reads when caller user small buffer.
- Add support for file related to document. The files are automatically removed when the related document is.

### Changed

- *Collection.SetBleveIndex now takes a *mapping.DocumentMapping which is used as *index.IndexImpl.DefaultMapping.
  This makes the reindexing more reliable and the use of the field more user friendly.

## [0.3.2](https://github.com/alexandrestein/gotinydb/compare/v0.3.2...v0.3.3)

### Fixes

- Bug where the database does not load the parameters properly.

## [0.3.2](https://github.com/alexandrestein/gotinydb/compare/v0.3.1...v0.3.2)

### Fixes

- Error fixed after database restore.

## [0.3.1](https://github.com/alexandrestein/gotinydb/compare/v0.3.0...v0.3.1)

### Add

- Support for partial file reads and writes [fixes #17](https://github.com/alexandrestein/gotinydb/issues/17).
- Support batch writes, [fixes #19](https://github.com/alexandrestein/gotinydb/issues/19).
- Support multiple get at ones.
- Add collection and file iterator fixes [#18](https://github.com/alexandrestein/gotinydb/issues/18).
- Add file lock to prevent concurrent writes on a single file.

## Remove

- Clean [Dep](https://github.com/golang/dep) files.

## [[0.3.0]](https://github.com/alexandrestein/gotinydb/compare/v0.2.2...v0.3.0) - 2018-10-09

### Changed

- Rewrites of the package to get read of all prototyping.
- All indexing relies on [Bleve](https://blevesearch.com).

## [[0.2.2]](https://github.com/alexandrestein/gotinydb/compare/v0.2.1...v0.2.2) - 2018-09-21

### Changed

- ENCRYPTION: Key management is redesigned to protect the nonce. A random part is generated for every writes and is used to derive the nonce.
- Optimized the file testing to be more memory efficient and much faster. So short skip has been removed.
- Upgrade dependencies.
- NewQuery in now *Collection.NewQuery to use the configured settings by default.

### Add

- ENCRYPTION: support for update the encryption key.
- Links in the changelog to provide easy diff file with github compare.
- Exists filter objects which has the requested field.
- Contains filter objects when the requested values is some where inside the field value.

## [[0.2.1]](https://github.com/alexandrestein/gotinydb/compare/v0.2.0...v0.2.1) - 2018-09-20

### Fixed

- Race condition.

## [[0.2.0]](https://github.com/alexandrestein/gotinydb/compare/v0.1.0...v0.2.0) - 2018-09-20

### Changed

- Encryption replace the data integrity check. Every thing which goes into the database is encrypted.

### Add

- Add a loop that run garbage collection with a configurable timer.

## [[0.1.0]](https://github.com/alexandrestein/gotinydb/compare/v0.1.0...v0.2.0) - 2018-09-19

### Changed

- Filter are structs and not interface any more and the exclusion filters has been modified.
- Many modification at the API.

### Remove

- Bolt database.

## [[0.0.8]](https://github.com/alexandrestein/gotinydb/compare/v0.0.7...v0.0.8)- 2018-09-07

### Added

- Add support to write and read big content.
- Add support for multiple write operation at ones [#11](https://github.com/alexandrestein/gotinydb/issues/11).

### Changed

- Filters have been rethink to be more user friendly.
- The test has been fully remade to get it more relevant: [#7](https://github.com/alexandrestein/gotinydb/issues/7)
- Make a queue to serialize the write when possible. This is related to [#11](https://github.com/alexandrestein/gotinydb/issues/11)
- Make a specific index type for unsigned integer values
- Deletions are serialized as the insertions

### Fixes

- Clean tests after complete. Fixes [#1](https://github.com/alexandrestein/gotinydb/issues/1).

## [[0.0.7]](https://github.com/alexandrestein/gotinydb/compare/v0.0.6...v0.0.7) - 2018-08-16

### Added

- Add support for exclusion filter

### Changed

- String indexed values need to contains the filter value to be referenced

## [[0.0.6]](https://github.com/alexandrestein/gotinydb/compare/v0.0.5...v0.0.6) - 2018-08-14

### Fixed

- Bug fixe with index range not working

## [[0.0.5]](https://github.com/alexandrestein/gotinydb/compare/v0.0.4...v0.0.5) - 2018-08-03

### Added

- Add CHANGELOG.md.
- Methods to show collections and indexes informations.
- Index values into slices

### Changed

- String indexes are case sensitive. It was made unsensitive on purpose but it's better that caller take care of it he wants case unsensitive indexing.

### Fixed

- Consistent indexing with JSON tags. [#6](https://github.com/alexandrestein/gotinydb/issues/6)

## [0.0.4] - 2018-08-03