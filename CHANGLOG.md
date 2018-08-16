# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## Added

- Add support for exclusion filter 

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