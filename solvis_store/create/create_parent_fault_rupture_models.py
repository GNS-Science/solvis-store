import logging
import time
from functools import lru_cache
from typing import Iterable, Iterator, Set, Tuple

from solvis import FaultSystemSolution

from solvis_store import model

log = logging.getLogger(__name__)


# TODO: this is here temporarily until we can get solvis published (GHA problems)
def parent_faults(sol: FaultSystemSolution) -> Iterable[Tuple[int, str]]:
    return zip(list(sol.fault_sections.ParentID.unique()), list(sol.fault_sections.ParentName.unique()))


@lru_cache
def get_rupture_ids_for_parent_fault(fault_system_solution: FaultSystemSolution, fault_name: str) -> Set[int]:
    rr = set(fault_system_solution.ruptures_with_rates["Rupture Index"].unique())
    pr = set(fault_system_solution.get_ruptures_for_parent_fault(fault_name))  # all, including those with no rate
    return rr.intersection(pr)


def create_parent_fault_rupture_models(
    fault_system_solution: FaultSystemSolution, rupture_set_id: str, create_tables: bool = False
) -> Iterator[model.RuptureSetParentFaultRuptures]:

    log.debug('get_parent_fault_rupture_models')
    if create_tables:
        model.migrate()
    for fault in parent_faults(fault_system_solution):

        tic22 = time.perf_counter()
        fault_rupture_ids = [int(rid) for rid in get_rupture_ids_for_parent_fault(fault_system_solution, fault[1])]
        tic23 = time.perf_counter()
        log.debug('fss.get_ruptures_for_parent_fault %s: %2.3f seconds' % (fault[0], (tic23 - tic22)))

        yield model.RuptureSetParentFaultRuptures(
            rupture_set_id=rupture_set_id,
            fault_name=fault[1],
            fault_id=int(fault[0]),
            ruptures=fault_rupture_ids,
            rupture_count=len(fault_rupture_ids),
        )
