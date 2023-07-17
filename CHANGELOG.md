# Changelog

## [2.0.0] - 2023-07-17
### Added
 - new fault_name model & queries
### Changed
 - solvis dependency >= 0.7.0
 - API changes
    - get_ruptures -> query.get_location_radius_ruptures
    - get_rupture_ids -> query.get_location_radius_rupture_ids

## [1.1.0] - 2023-04-24
### Added
 - poetry run cli -> scripts/cli.py
 - for extra dependencies install with `poetry install --with scripts`
 - get_ruptures query
 - get_rupture_ids query

### Changed
 - requires nzshm-model >=0.3.0

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
