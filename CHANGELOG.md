# Changelog

## [1.1.x] - 2023-04-
### Added
 - poetry run cli -> scripts/cli.py
 - for extra dependencies install with `poetry install --with scripts`

## [1.0.1] - 2023-04-19
### Removed
 - geopandas dependency

## [1.0.0] - 2023-04-19
### Changed
 - major breaking change, basically it's a new API, focussing solely on Distances for RuptureSets with Locations
 - class RuptureSetLocationRadiusRuptures is superceded by RuptureSetLocationDistances
 - other models and supporting code are gone

### Added
 - more testing for set operation union vs intersection

## Removed
 - old models etc

## [0.1.1] - 2023-02-21
### Changed
 - update github actions helper
 - udpate python versions for test and release
 - refactor model.attributes package
 - more test coverage
### Added
 - `.env` file for poetry testing `poetry run pytest`

## 0.1.0 - 2022-12-20

* First release on PyPI.
