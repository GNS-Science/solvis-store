"""Console script for solvis."""
# noqa
import logging
import sys
import pathlib
import click
import nzshm_model

from solvis import CompositeSolution
from solvis_store import create

from nzshm_common.location.location import LOCATION_LISTS

SKIP_FS_NAMES = ['SLAB']

log = logging.getLogger()
# logging.basicConfig(level=logging.DEBUG)
logging.getLogger('nshm_toshi_client.toshi_client_base').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
# logging.getLogger('pynamodb').setLevel(logging.DEBUG)
logging.getLogger('fiona').setLevel(logging.INFO)
logging.getLogger('gql.transport.requests').setLevel(logging.WARN)
logging.getLogger('solvis').setLevel(logging.INFO)
logging.getLogger('solvis_store').setLevel(logging.DEBUG)

formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
screen_handler = logging.StreamHandler(stream=sys.stdout)
screen_handler.setFormatter(formatter)
log.addHandler(screen_handler)

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
@click.option('--dry_run', '-D', is_flag=True, help="do everything except save the data")
@click.option('--create_tables', '-T', is_flag=True, help="ensure that the tables exist")
@click.pass_context
def radius(ctx, archive_path, model_id, dry_run, create_tables):
    """Create pynamoDB records for rupture sets based on radius from standard locations.

    from the CompositeSolution file at ARCHIVE_PATH, using model model_id

    NB these two must be compatible.
    """
    assert pathlib.Path(archive_path).exists()

    current_model = nzshm_model.get_model_version(model_id)
    slt = current_model.source_logic_tree()

    # get the composite solution
    comp = CompositeSolution.from_archive(pathlib.Path(archive_path), slt)

    for fslt in slt.fault_system_lts:

        fault_system_key = fslt.short_name

        if fault_system_key in SKIP_FS_NAMES:  # CRU
            continue

        # check the solutions in a given fault system have the same rupture_set
        ruptset_ids = list(set([branch.rupture_set_id for branch in fslt.branches]))
        assert len(ruptset_ids) == 1
        rupture_set_id = ruptset_ids[0]

        # build the models
        for mod in create.create_location_radius_rupture_models(
            comp._solutions[fault_system_key],
            rupture_set_id,
            locations=LOCATION_LISTS['NZ']['locations'],
            distances=[10, 20, 30, 40, 50, 100, 200],
            create_tables=create_tables,
        ):
            print(mod, mod.radius, mod.rupture_count)
            if not dry_run:
                mod.save()


@cli.command()
@click.argument('archive_path')
@click.option('--model_id', '-M', default="NSHM_v1.0.4", help="default value is `NSHM_v1.0.4`")
@click.option('--dry_run', '-D', is_flag=True, help="do everything except save the data")
@click.option('--create_tables', '-T', is_flag=True, help="ensure that the tables exist")
@click.pass_context
def parents(ctx, archive_path, model_id, dry_run, create_tables):
    """Create pynamoDB records for rupture sets tha include each parent fault name.

    from the CompositeSolution file at ARCHIVE_PATH, using model model_id

    NB these two must be compatible.
    """
    assert pathlib.Path(archive_path).exists()

    current_model = nzshm_model.get_model_version(model_id)
    slt = current_model.source_logic_tree()

    # get the composite solution
    comp = CompositeSolution.from_archive(pathlib.Path(archive_path), slt)

    fslt = None
    for fslt in slt.fault_system_lts:
        if fslt.short_name == 'CRU':
            break

    if not fslt:
        raise ValueError('invalid solution archive.')

    fault_system_key = fslt.short_name

    # check the solutions in a given fault system have the same rupture_set
    ruptset_ids = list(set([branch.rupture_set_id for branch in fslt.branches]))
    assert len(ruptset_ids) == 1
    rupture_set_id = ruptset_ids[0]

    fss = comp._solutions[fault_system_key]

    for mod in create.create_parent_fault_rupture_mods(fss, rupture_set_id, create_tables):
        print(mod, mod.fault_name, mod.fault_id, mod.rupture_count)
        if not dry_run:
            mod.save()


if __name__ == "__main__":
    cli()  # pragma: no cover
