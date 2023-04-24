import logging
from datetime import datetime as dt
from functools import lru_cache
from typing import Iterable, Iterator, NamedTuple, Set, Tuple

from solvis_store import model

from .cloudwatch import ServerlessMetricWriter
from .config import CLOUDWATCH_APP_NAME

log = logging.getLogger(__name__)

mRLR = model.RuptureSetLocationDistances

db_metrics = ServerlessMetricWriter(lambda_name=CLOUDWATCH_APP_NAME, metric_name="MethodDuration")
# db_metrics_hr = ServerlessMetricWriter(lambda_name=CLOUDWATCH_APP_NAME, metric_name="MethodDuration", resolution=1)


class RuptureIndexLocationDistance(NamedTuple):
    rupt_id: int
    location_id: str
    distance: float = float('nan')


@lru_cache(maxsize=256)
def query_fn(rupture_set_id, loc, radius):
    return [i for i in mRLR.query(f'{rupture_set_id}', mRLR.location_radius == (f"{loc}:{radius}"))]


@lru_cache(maxsize=256)
def get_the_ids(rupture_set_id: str, locations: Tuple[str], radius: int, union: bool) -> Set[int]:
    """get the set of rupture ids matching the query args"""
    first_set = True

    for loc in locations:
        items = query_fn(rupture_set_id, loc, radius)

        assert len(items) in [0, 1]

        if len(items) == 0 and not union:
            return set()

        for item in items:
            log.debug(
                f'SLR query item: {item} {item.location_radius}, '
                f'ruptures: {len(item.ruptures)}  examples: {list(item.ruptures)[:10]}'
            )

            if first_set:
                rupt_ids = set()
                if not union:
                    rupt_ids = set(item.ruptures)

                first_set = False

            if union:
                rupt_ids = rupt_ids.union(set(item.ruptures))
            else:
                rupt_ids = rupt_ids.intersection(set(item.ruptures))

            if not union and len(rupt_ids) == 0:
                log.info('break now, no point querying further')
                break

            log.debug(rupt_ids)

    log.debug(f'get_the_rupt_ids({locations}) returns {len(list(rupt_ids))} rupture ids')
    return rupt_ids


# QUERY operations for the API get endpoint(s)
def get_ruptures(
    rupture_set_id: str, locations: Tuple[str], radius: int, union: bool = False
) -> Iterator[RuptureIndexLocationDistance]:

    t0 = dt.utcnow()

    log.debug(f'get_rupture_ids({locations}, {radius}, union: {union})')

    def filter_ruptures(id_list: Iterable[int]) -> Iterator[RuptureIndexLocationDistance]:
        for loc in locations:
            items = query_fn(rupture_set_id, loc, radius)
            for item in items:
                log.debug(
                    f'SLR query item: {item} {item.location_radius}, '
                    f'ruptures: {len(item.ruptures)}  examples: {list(item.ruptures)[:10]}'
                )
                # if not mapping.get(loc):
                #     mapping[loc] = list()

                # location = mapping.get(loc)
                distances = list(item.distances)
                for idx, rupt_id in enumerate(item.ruptures):
                    if rupt_id in id_list:
                        yield (RuptureIndexLocationDistance(rupt_id=rupt_id, location_id=loc, distance=distances[idx]))

    filtered_ids = get_the_ids(rupture_set_id, locations, radius, union)

    ruptures = filter_ruptures(filtered_ids)

    t1 = dt.utcnow()
    db_metrics.put_duration(__name__, 'get_ruptures', t1 - t0)
    return ruptures


# QUERY operations for the API get endpoint(s)
def get_rupture_ids(rupture_set_id: str, locations: Tuple[str], radius: int, union: bool = False) -> Set[int]:

    t0 = dt.utcnow()

    log.debug(f'get_rupture_ids({locations}, {radius}, union: {union})')

    ids = get_the_ids(rupture_set_id, locations, radius, union)

    t1 = dt.utcnow()
    db_metrics.put_duration(__name__, 'get_rupture_ids', t1 - t0)
    return ids
