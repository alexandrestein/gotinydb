# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/alexandrestein/gotinydb/compare/v0.2.1...dev)

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