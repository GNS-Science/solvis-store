"""Console script for solvis."""
# noqa
import logging
import os
import sys
import json
import pathlib
import click
import nzshm_model

from pyproj import Transformer

# from solvis.get_secret import get_secret
from solvis import geometry
from solvis import CompositeSolution, FaultSystemSolution
from solvis_store import model
from nzshm_common.location.location import location_by_id, LOCATION_LISTS

from typing import Iterator, List

SKIP_FS_NAMES =['SLAB'] #, 'CRU'

log = logging.getLogger()
logging.basicConfig(level=logging.INFO)
logging.getLogger('nshm_toshi_client.toshi_client_base').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
#logging.getLogger('pynamodb').setLevel(logging.DEBUG)
logging.getLogger('fiona').setLevel(logging.INFO)
logging.getLogger('gql.transport.requests').setLevel(logging.WARN)

formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
screen_handler = logging.StreamHandler(stream=sys.stdout)
screen_handler.setFormatter(formatter)
log.addHandler(screen_handler)

def create_models(fault_system_solution: FaultSystemSolution, rupture_set_id: str, locations: List[str], distances:List[int], create_tables: bool = False) -> Iterator[model.RuptureSetLocationDistances]:
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
            print('RADIUS', radius_km)
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



#  _ __ ___   __ _(_)_ __
# | '_ ` _ \ / _` | | '_ \
# | | | | | | (_| | | | | |
# |_| |_| |_|\__,_|_|_| |_|

@click.group()
def cli():
    click.echo("solvis-store tasks - populate store.")
    pass

@cli.command()
@click.argument('archive_path')
@click.option('--model_id', '-M', default="NSHM_v1.0.4", help="default value is `NSHM_v1.0.4`")
@click.option('--dry_run', '-D', is_flag=True, help="actually save the data")
@click.option('--create_tables', '-T', is_flag=True, help="check that the tables exist")
@click.pass_context
def populate(ctx, archive_path, model_id, dry_run, create_tables):
    """Create pynamoDB records for rupture sets from the CompositeSolution file at
    ARCHIVE_PATH, using model model_id.

    NB these two must be compatible.
    """
    assert pathlib.Path(archive_path).exists()

    current_model = nzshm_model.get_model_version(model_id)
    slt = current_model.source_logic_tree()

    # get the composite solution
    comp = CompositeSolution.from_archive(pathlib.Path(archive_path), slt)

    for fslt in slt.fault_system_lts:

        fault_system_key = fslt.short_name

        if fault_system_key in SKIP_FS_NAMES: #CRU
            continue

        # check the solutions in a given fault system have the same rupture_set
        ruptset_ids = list(set([branch.rupture_set_id for branch in fslt.branches]))
        assert len(ruptset_ids) == 1
        rupture_set_id = ruptset_ids[0]

        #build the models
        for model in create_models(comp._solutions[fault_system_key], rupture_set_id, locations=LOCATION_LISTS['NZ']['locations'], distances=[10, 20, 50, 100, 200], create_tables=create_tables):
            print(model, model.radius, model.rupture_count)
            if not dry_run:
                model.save()

if __name__ == "__main__":
    cli()  # pragma: no cover
