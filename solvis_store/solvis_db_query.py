import logging
from datetime import datetime as dt
from functools import lru_cache
from typing import Iterator, List, NamedTuple, Set

from solvis_store import model

from .cloudwatch import ServerlessMetricWriter
from .config import CLOUDWATCH_APP_NAME

log = logging.getLogger(__name__)

mRLR = model.RuptureSetLocationDistances

db_metrics = ServerlessMetricWriter(lambda_name=CLOUDWATCH_APP_NAME, metric_name="MethodDuration")
# db_metrics_hr = ServerlessMetricWriter(lambda_name=CLOUDWATCH_APP_NAME, metric_name="MethodDuration", resolution=1)


class RuptureIndexDistance(NamedTuple):
    rupt_id: int
    distance: float = float('nan')


def id_distance_tuples(item: model.RuptureSetLocationDistances) -> Iterator[RuptureIndexDistance]:
    if item.distances is None:
        for rupt_id in item.ruptures:
            yield RuptureIndexDistance(rupt_id=rupt_id)
    else:
        for idx, rupt_id in enumerate(item.ruptures):
            yield RuptureIndexDistance(rupt_id=int(rupt_id), distance=list(item.distances)[idx])


# QUERY operations for the API get endpoint(s)
def get_rupture_ids(
    rupture_set_id: str, locations: List[str], radius: int, union: bool = False
) -> Set[RuptureIndexDistance]:

    t0 = dt.utcnow()

    log.debug(f'get_rupture_ids({locations}, {radius}, union: {union})')

    @lru_cache(maxsize=256)
    def query_fn(rupture_set_id, loc, radius):
        return [i for i in mRLR.query(f'{rupture_set_id}', mRLR.location_radius == (f"{loc}:{radius}"))]

    def get_the_ids(locations):
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
                    tuples = set()
                    if not union:
                        tuples = set(id_distance_tuples(item))
                    first_set = False

                if union:
                    tuples = tuples.union(set(id_distance_tuples(item)))
                else:
                    tuples = tuples.intersection(set(id_distance_tuples(item)))

                if not union and len(tuples) == 0:
                    log.info('break now, no point querying further')
                    break

                log.debug(tuples)

        log.debug(f'get_the_tuples({locations}) returns {len(list(tuples))} rupture tuples')
        return tuples

    ids = get_the_ids(locations)

    t1 = dt.utcnow()
    db_metrics.put_duration(__name__, 'get_rupture_ids', t1 - t0)
    return ids
