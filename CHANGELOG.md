# Changelog

## [2.0.4] - 2024-07-04

### Changed
 - update dependencies
   - solvis to >=0.11.1
   - nzshm-common = "^0.8.1"
   - nzshm-model = "^0.10.6"
 - move scripts package into solvis_store package
 - update testing instructions in README
 - remove twine and docs from toc build step
 - add python 3.12 to tox setup
 - add docs page for scripts

## [2.0.3] - 2024-06-10
### Changed
 - update solvis to 0.11.1
 - update nzshm-model to 0.4.0
 - update mkdocs to 1.6 (along with extension sdependencies)
 - Supporting Python version 3.9, 3.10, 3.11.

## [2.0.2] - 2023-08-02
### Changed
 - fix for union operation on get_location_radius_rupture_ids

## [2.0.1] - 2023-07-18
### Changed
 - update solvis and pynamodb versions
 - minor CLI output improvement
 - fixed query syntax for fault_name

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
