import logging
from typing import Dict, Iterator, List

import solvis
from solvis_store import model

log = logging.getLogger(__name__)


def clean_slate():
    model.drop_all()
    model.migrate()


def get_location_radius_rupture_models(
    solution_id: str, sol: solvis.InversionSolution, locations: List[Dict], radii: List[int]
) -> Iterator[model.RuptureSetLocationDistances]:
    log.debug('get_location_radius_rupture_models')
    for item in locations:

        for radius in radii:
            polygon = solvis.circle_polygon(radius_m=radius, lat=item['latitude'], lon=item['longitude'])
            rupts = set(sol.get_ruptures_intersecting(polygon).tolist())

            loc = item['id']

            if len(rupts) > 1e5:
                raise Exception(f"Too many ruptures in {loc} with radius {radius}: {len(rupts)}")

            yield model.RuptureSetLocationDistances(
                location_radius=f"{loc}:{int(radius)}",
                solution_id=solution_id,
                radius=int(radius),
                location=loc,
                ruptures=rupts,
                rupture_count=len(rupts),
            )


# # sol, locations, radii
# def save_solution_location_radii(solution_id: str, models: List[model.RuptureSetLocationRadiusRuptures]):
#     log.debug('save_solution_location_radii')
#     with model.RuptureSetLocationRadiusRuptures.batch_write() as batch:
#         for item in models:
#             # print(item)
#             # item.save()
#             batch.save(item)
