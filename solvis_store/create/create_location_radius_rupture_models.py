import logging
from typing import Iterator, List

from nzshm_common.location.location import location_by_id
from pyproj import Transformer

# from solvis.get_secret import get_secret
from solvis import FaultSystemSolution, geometry

from solvis_store import model

log = logging.getLogger(__name__)


def create_location_radius_rupture_models(
    fault_system_solution: FaultSystemSolution,
    rupture_set_id: str,
    locations: List[str],
    distances: List[int],
    create_tables: bool = False,
) -> Iterator[model.RuptureSetLocationDistances]:
    """
    Generate models for the ruptures having any fault_sections that are within the distance to location arguments.
    """
    fss = fault_system_solution
    if create_tables:
        model.migrate()

    for loc_id in locations:

        loc = location_by_id(loc_id)

        # calculate the distances
        lon, lat = loc['longitude'], loc['latitude']
        wgs84_projection = "+proj=longlat +datum=WGS84 +no_defs"
        local_azimuthal_projection = "+proj=aeqd +R=6371000 +units=m +lat_0={} +lon_0={}".format(lat, lon)
        transformer = Transformer.from_crs(wgs84_projection, local_azimuthal_projection)

        gdf = fss.fault_surfaces()
        gdf['distance_km'] = gdf.apply(
            lambda section: geometry.section_distance(transformer, section.geometry, section.UpDepth, section.LowDepth),
            axis=1,
        )

        # loop through the distances
        for radius_km in distances:
            log.debug('RADIUS %s' % radius_km)
            # get the sections whose closest point is within radius
            sections_gdf = gdf[gdf.distance_km < radius_km]

            # get the ruptures
            rupts_gdf = fss.rupture_sections.join(sections_gdf, 'section', how='inner')

            # aggregate to get the closest rupture/section for each rupture
            rupt_min_dist_df = rupts_gdf.pivot_table(index=['rupture'], aggfunc={"distance_km": 'min'})

            ## filter out ruptures with no rates
            rates_df = fss.rates.copy().reset_index(level=0, drop=True).drop(columns=['fault_system', 'Rupture Index'])
            rupts_rates_gdf = rates_df.join(rupt_min_dist_df, 'Rupture Index', how='inner')

            if not len(rupts_rates_gdf):
                continue

            if len(rupts_rates_gdf) > 1e5:
                raise Exception(f"Too many ruptures in {loc} with radius {radius_km}: {len(rupts_rates_gdf)}")

            # get the list of rupture ids
            ruptures = list(rupts_rates_gdf.index.astype(dtype='int').tolist())

            yield model.RuptureSetLocationDistances(
                rupture_set_id=rupture_set_id,
                location_radius=f'{loc_id}:{radius_km}',
                radius=radius_km,
                location=loc_id,
                ruptures=ruptures,
                distances=[round(d, 3) for d in rupts_rates_gdf.distance_km.to_list()],
                rupture_count=len(ruptures),
            )
