import logging
from datetime import datetime as dt
from functools import lru_cache
from typing import Iterable, Iterator, List, NamedTuple, Set, Tuple

from solvis_store import model
from solvis_store.cloudwatch import ServerlessMetricWriter
from solvis_store.config import CLOUDWATCH_APP_NAME

log = logging.getLogger(__name__)

# mRLR = model.RuptureSetLocationDistances
mFNR = model.RuptureSetParentFaultRuptures

db_metrics = ServerlessMetricWriter(lambda_name=CLOUDWATCH_APP_NAME, metric_name="MethodDuration")
# db_metrics_hr = ServerlessMetricWriter(lambda_name=CLOUDWATCH_APP_NAME, metric_name="MethodDuration", resolution=1)


class RuptureIndexFault(NamedTuple):
    rupt_id: int
    fault_id: int
    fault_name: str


@lru_cache(maxsize=256)
def query_fn(rupture_set_id: str, fault_names: Tuple[str]) -> List[mFNR]:
    log.debug(f'query_fn: {rupture_set_id} {fault_names}')
    items: List[mFNR] = []
    for fault_name in fault_names:
        for itm in mFNR.query(rupture_set_id, mFNR.fault_name == fault_name):
            items.append(itm)
    return items


@lru_cache(maxsize=256)
def get_the_ids(rupture_set_id: str, fault_names: Tuple[str], union: bool) -> Set[int]:
    """get the set of rupture ids matching the query args"""
    first_set = True
    items = query_fn(rupture_set_id, fault_names)
    for item in items:
        log.debug(
            f'SLR query item: {item} {item.fault_name}, '
            f'ruptures: {len(item.ruptures)}  examples: {list(item.ruptures)[:10]}'
        )
        int_rupt_ids = [int(itm) for itm in item.ruptures]
        if first_set:
            rupt_ids: Set[int] = set(int_rupt_ids)
            first_set = False
            continue

        if union:
            rupt_ids = rupt_ids.union(set(int_rupt_ids))
        else:
            rupt_ids = rupt_ids.intersection(set(int_rupt_ids))
        if not union and len(rupt_ids) == 0:
            log.info('break now, no point querying further')
            break
        log.debug(rupt_ids)

    log.debug(f'get_the_ids({fault_names}) returns {len(list(rupt_ids))} rupture ids')
    return rupt_ids


# QUERY operations for the API get endpoint(s)
def get_fault_name_ruptures(
    rupture_set_id: str, fault_names: Iterable[str], union: bool = False
) -> Iterator[RuptureIndexFault]:

    t0 = dt.utcnow()

    log.debug(f'get_fault_name_ruptures({fault_names}, union: {union})')

    def filter_ruptures(id_list: Iterable[int]) -> Iterator[RuptureIndexFault]:
        items = query_fn(rupture_set_id, tuple(fault_names))
        for item in items:
            log.debug(
                f'SLR query item: {item} {item.fault_name}, '
                f'ruptures: {len(item.ruptures)}  examples: {list(item.ruptures)[:10]}'
            )

            for idx, rupt_id in enumerate(item.ruptures):
                if int(rupt_id) in id_list:
                    yield (
                        RuptureIndexFault(rupt_id=int(rupt_id), fault_id=int(item.fault_id), fault_name=item.fault_name)
                    )

    filtered_ids: Iterable[int] = get_the_ids(rupture_set_id, tuple(fault_names), union)
    ruptures = filter_ruptures(filtered_ids)

    t1 = dt.utcnow()
    db_metrics.put_duration(__name__, 'get_fault_name_ruptures', t1 - t0)
    return ruptures


# QUERY operations for the API get endpoint(s)
def get_fault_name_rupture_ids(rupture_set_id: str, fault_names: Iterable[str], union: bool = False) -> Set[int]:

    t0 = dt.utcnow()

    log.debug(f'get_fault_name_rupture_ids({fault_names}, union: {union})')

    ids = get_the_ids(rupture_set_id, tuple(fault_names), union)

    t1 = dt.utcnow()
    db_metrics.put_duration(__name__, 'get_fault_name_rupture_ids', t1 - t0)
    return ids
