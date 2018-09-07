# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Add support for multiple write operation at ones [#11](https://github.com/alexandrestein/gotinydb/issues/11).

### Changed

- The test has been fully remade to get it more relevant: [#7](https://github.com/alexandrestein/gotinydb/issues/7)
- Make a queue to serialize the write when possible. This is related to [#11](https://github.com/alexandrestein/gotinydb/issues/11)
- Make a specific index type for unsigned integer values
- Deletions are serialized as the insertions

### Fixes

- Clean tests after complete. Fixes [#1](https://github.com/alexandrestein/gotinydb/issues/1).

## [0.0.7] - 2018-08-16

### Added

- Add support for exclusion filter

### Changed

- String indexed values need to contains the filter value to be referenced

## [0.0.6] - 2018-08-14

### Fixed

- Bug fixe with index range not working

## [0.0.5] - 2018-08-03

### Added

- Add CHANGELOG.md.
- Methods to show collections and indexes informations.
- Index values into slices

### Changed

- String indexes are case sensitive. It was made unsensitive on purpose but it's better that caller take care of it he wants case unsensitive indexing.

### Fixed

- Consistent indexing with JSON tags. [#6](https://github.com/alexandrestein/gotinydb/issues/6)

## [0.0.4] - 2018-08-03