import logging
from datetime import datetime as dt
from functools import lru_cache
from typing import List, Set, NamedTuple, Iterator

import geopandas as gpd
import pandas as pd

from solvis_store import model

from .cloudwatch import ServerlessMetricWriter
from .config import CLOUDWATCH_APP_NAME

log = logging.getLogger(__name__)

mRLR = model.RuptureSetLocationRadiusRuptures
mRFS = model.RuptureSetFaultSection
mSR = model.SolutionRupture

db_metrics = ServerlessMetricWriter(lambda_name=CLOUDWATCH_APP_NAME, metric_name="MethodDuration")
db_metrics_hr = ServerlessMetricWriter(lambda_name=CLOUDWATCH_APP_NAME, metric_name="MethodDuration", resolution=1)


class RuptureIndexDistance(NamedTuple):
    rupt_id: int
    distance: float = float('nan')


def id_distance_tuples(item: model.RuptureSetLocationRadiusRuptures) -> Iterator[RuptureIndexDistance]:
    if item.distances is None:
        for rupt_id in item.ruptures:
            yield RuptureIndexDistance(rupt_id=rupt_id)
    else:
        for idx, rupt_id in enumerate(item.ruptures):
            yield RuptureIndexDistance(rupt_id=rupt_id, distance=item.distances[idx])

# QUERY operations for the API get endpoint(s)
def get_rupture_ids(rupture_set_id: str, locations: List[str], radius: int, union: bool = False) -> Set[RuptureIndexDistance]:

    t0 = dt.utcnow()

    log.debug(f'get_rupture_ids({locations}, {radius}, union: {union})')

    @lru_cache(maxsize=256)
    def query_fn(rupture_set_id, loc, radius):
        return [i for i in mRLR.query(f'{rupture_set_id}', mRLR.location_radius == (f"{loc}:{radius}"))]

    def get_the_ids(locations):
        first_set = True
        tuples = list()
        ids = set()
        for loc in locations:
            items = query_fn(rupture_set_id, loc, radius)

            assert len(items) in [0, 1]

            if len(items) == 0 and not union:
                return set()

            for item in items:
                if not item.ruptures:
                    if not union:
                        # It is valid for the attribute to be None - this dimension has no ruptures
                        return set()
                    else:
                        continue

                log.debug(
                    f'SLR query item: {item} {item.location_radius}, '
                    'ruptures: {len(item.ruptures)}  examples: {list(item.ruptures)[:10]}'
                )

                tuples = id_distance_tuples(item.ruptures)
                tuple_ids = set([t.rupt_id for t in tuples])
                if first_set:
                    ids = set(tuple_ids)
                    first_set = False
                else:
                    if union:
                        ids = tuples.union(id_distance_tuples(item.ruptures))
                    else:
                        tuples = tuples.intersection(id_distance_tuples(item.ruptures))

        log.debug(f'get_the_tuples({locations}) returns {len(list(tuples))} rupture tuples')
        return tuples

    ids = get_the_ids(locations)

    t1 = dt.utcnow()
    db_metrics.put_duration(__name__, 'get_rupture_ids', t1 - t0)
    return ids


@lru_cache(maxsize=256)
def get_ruptures_in(solution_id: str, rupture_ids: tuple) -> gpd.GeoDataFrame:
    log.debug(f">>> get_ruptures_in({solution_id}...)")
    t0 = dt.utcnow()
    index = []
    values = []

    # The list can contain up to 100 values, separated by commas.
    log.debug(rupture_ids)

    chunksize = 100
    log.debug(f'range {int(len(rupture_ids)/chunksize)}')
    for i in range(int(len(rupture_ids) / chunksize) + 1):
        idx = i * chunksize
        ids = rupture_ids[idx : idx + chunksize]
        t00 = dt.utcnow()
        for item in mSR.query(f'{solution_id}', filter_condition=mSR.rupture_index.is_in(*ids)):
            values.append(item.attribute_values)
            index.append(item.rupture_index)
        db_metrics_hr.put_duration(__name__, 'get_ruptures_in[query_chunk]', dt.utcnow() - t00)

    db_metrics.put_duration(__name__, 'get_ruptures_in', dt.utcnow() - t0)
    log.debug(item.attribute_values)
    log.debug(f">>> get_ruptures_in: index has {len(index)}; values has {len(values)}")
    return pd.DataFrame(values, index=index)


def get_all_solution_ruptures(solution_id: str) -> gpd.GeoDataFrame:

    log.debug(f">>> get_all_solution_ruptures({solution_id})")
    t0 = dt.utcnow()
    index = []
    values = []

    for item in mSR.query(solution_id):
        values.append(item.attribute_values)
        index.append(item.rupture_index)

    db_metrics.put_duration(__name__, 'get_all_solution_ruptures', dt.utcnow() - t0)
    log.debug(f">>> get_all_solution_ruptures: index has {len(index)}; values has {len(values)}")
    return pd.DataFrame(values, index=index)


@lru_cache(maxsize=32)
def get_ruptures(solution_id: str) -> gpd.GeoDataFrame:
    log.debug(f">>> get_ruptures({solution_id})")
    t0 = dt.utcnow()
    index = []
    values = []
    for item in mSR.query(f'{solution_id}'):
        values.append(item.attribute_values)
        index.append(item.rupture_index)

    db_metrics.put_duration(__name__, 'get_ruptures', dt.utcnow() - t0)
    log.debug(item.attribute_values)
    log.debug(f">>> get_ruptures: now index has {len(index)}; values has {len(values)}")
    return pd.DataFrame(values, index=index)


@lru_cache(maxsize=32)
def get_fault_sections(rupture_set_id: str) -> gpd.GeoDataFrame:
    log.debug(f">>> get_fault_sections({rupture_set_id})")
    t0 = dt.utcnow()
    index = []
    values = []
    for item in mRFS.query(f'{rupture_set_id}'):
        values.append(item.attribute_values)
        index.append(item.section_index)
    db_metrics_hr.put_duration(__name__, 'get_fault_sections[query]', dt.utcnow() - t0)
    log.debug(f">>> get_fault_sections: post query, index has {len(index)}; values has {len(values)}")
    df = pd.DataFrame(values, index)
    return gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df.geometry))


@lru_cache(maxsize=32)
def matched_rupture_sections_gdf(
    solution_id: str,
    rupture_set_id: str,
    locations: str,
    radius: int,
    min_rate: float,
    max_rate: float,
    min_mag: float,
    max_mag: float,
    union: bool = False,
) -> gpd.GeoDataFrame:

    t0 = dt.utcnow()
    locs: List[str] = locations.split(',')

    log.debug(locations)

    if locations:
        log.debug('Intersection/Union')
        ids = get_rupture_ids(rupture_set_id, locs, int(radius), union)
        if not ids:
            log.info(f"No rupture ids were returned for {rupture_set_id}, {locs}, {int(radius)}, {union}")
            return
        t1 = dt.utcnow()
        log.info(f'get_rupture_ids() (not cached), took {t1-t0}')
        ruptures_df = get_ruptures_in(solution_id, tuple(ids))
    else:  # no filter by location/radis
        log.debug('All ruptures')
        t1 = dt.utcnow()
        ruptures_df = get_all_solution_ruptures(solution_id)


    t2 = dt.utcnow()
    log.info(f'get_ruptures_in() (maybe cached), took {t2-t1}')

    if min_rate:
        log.debug(f"apply min rate filter: min={min_rate}")
        ruptures_df = ruptures_df[ruptures_df.annual_rate > min_rate]

    if max_rate:
        log.debug(f"apply max rate filter: max={max_rate}")
        ruptures_df = ruptures_df[ruptures_df.annual_rate < max_rate]

    if min_mag:
        log.debug(f"apply min magnitude filter: min={min_mag}")
        ruptures_df = ruptures_df[ruptures_df.magnitude > min_mag]

    if max_mag:
        log.debug(f"apply max magnitude filter: max={max_mag}")
        ruptures_df = ruptures_df[ruptures_df.magnitude < max_mag]

    t3 = dt.utcnow()
    log.info(f'apply filters  took {t3-t2}')

    if ruptures_df.empty:
        return ruptures_df

    log.debug("Build RuptureSections df")

    def build_rupture_sections_df(ruptures_df: pd.DataFrame) -> pd.DataFrame:
        table = []
        for row in ruptures_df.itertuples():
            rupture_id = row[0]
            fault_sections = row[4]
            for section_id in fault_sections:
                table.append(dict(rupture_index=rupture_id, section_index=section_id))

        return pd.DataFrame.from_dict(table)

    rupture_sections_df = build_rupture_sections_df(ruptures_df)

    t4 = dt.utcnow()
    log.info(f'apply build_rupture_sections_df (not cached), took {t4-t3}')

    sections_gdf = get_fault_sections(rupture_set_id)

    t5 = dt.utcnow()
    log.info(f'apply get_fault_sections (maybe cached), took {t5-t4}')

    log.debug("Assemble geojson")
    # join rupture details
    # log.debug(rupture_sections_df)
    # log.debug(ruptures_df)
    rupture_sections_df = rupture_sections_df.join(ruptures_df, 'rupture_index', how='inner', rsuffix='_R').drop(
        columns=['fault_sections', 'rupture_index_R']
    )

    # join fault_section details as GeoDataFrame
    rupture_sections_gdf = (
        gpd.GeoDataFrame(rupture_sections_df)
        .join(sections_gdf, 'section_index', how='inner', rsuffix='_R')
        .drop(columns=['rupture_set_id', 'rupture_set_id_R'])
    )

    t6 = dt.utcnow()
    log.info(f'Assemble geojson (not cached), took {t6-t5}')

    rupture_sections_gdf = rupture_sections_gdf.drop(
        columns=['area_m2', 'length_m', 'parent_id', 'parent_name', 'section_index_rk']#, 'solution_id_R']
    )

    # # Here we want to collapse all ruptures so we have just one feature for section. Each section can have the
    # count of ruptures, min, mean, max magnitudes & annual rates
    section_aggregates_gdf = rupture_sections_gdf.pivot_table(
        index=['section_index'], aggfunc=dict(annual_rate=['sum', 'min', 'max'], magnitude=['count', 'min', 'max'])
    )

    # join the rupture_sections_gdf details
    section_aggregates_gdf.columns = [".".join(a) for a in section_aggregates_gdf.columns.to_flat_index()]
    section_aggregates_gdf = section_aggregates_gdf.join(sections_gdf, 'section_index', how='inner', rsuffix='_R')

    print(section_aggregates_gdf.info())
    db_metrics.put_duration(__name__, 'matched_rupture_sections_gdf', dt.utcnow() - t0)
    return section_aggregates_gdf
