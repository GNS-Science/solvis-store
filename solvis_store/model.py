import logging
from datetime import datetime as dt

from pynamodb.attributes import ListAttribute, NumberAttribute, NumberSetAttribute, UnicodeAttribute
from pynamodb.models import Model

from .cloudwatch import ServerlessMetricWriter
from .config import CLOUDWATCH_APP_NAME, DEPLOYMENT_STAGE, IS_OFFLINE, IS_TESTING, REGION

log = logging.getLogger(__name__)

db_metrics = ServerlessMetricWriter(lambda_name=CLOUDWATCH_APP_NAME, metric_name="MethodDuration", resolution=1)


class MetricatedModel(Model):
    @classmethod
    def query(cls, *args, **kwargs):
        t0 = dt.utcnow()
        res = super(MetricatedModel, cls).query(*args, **kwargs)
        t1 = dt.utcnow()
        db_metrics.put_duration(__name__, f'{cls.__name__}.query', t1 - t0)
        return res


class RuptureSetLocationDistances(MetricatedModel):
    class Meta:
        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"SOLVIS_RuptureSetLocationDistances-{DEPLOYMENT_STAGE}"
        region = REGION

    rupture_set_id = UnicodeAttribute(hash_key=True)
    location_radius = UnicodeAttribute(range_key=True)  # eg WLG:100

    radius = NumberAttribute()
    location = UnicodeAttribute()
    ruptures = NumberSetAttribute()  # Rupture Index,
    distances = ListAttribute(of=NumberAttribute)  # distances, one for each rupture_index
    rupture_count = NumberAttribute()


class RuptureSetParentFaultRuptures(MetricatedModel):
    class Meta:
        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"SOLVIS_RuptureSetParentFaultRuptures-{DEPLOYMENT_STAGE}"
        region = REGION

    rupture_set_id = UnicodeAttribute(hash_key=True)
    fault_name = UnicodeAttribute(range_key=True)

    fault_id = NumberAttribute()
    ruptures = NumberSetAttribute()  # Rupture Index
    rupture_count = NumberAttribute()


table_classes = (RuptureSetLocationDistances, RuptureSetParentFaultRuptures)


def set_local_mode(host="http://localhost:8000"):
    if IS_OFFLINE and not IS_TESTING:
        log.info("Setting tables for local dynamodb instance in offline mode")
        for table in table_classes:
            table.Meta.host = host


def drop_all(*args, **kwargs):
    """
    drop all the tables etc
    """
    log.info("Drop all called")
    for table in table_classes:
        if table.exists():
            table.delete_table()
            log.info(f"deleted table: {table}")


def migrate(*args, **kwargs):
    """
    setup the tables etc

    NB: seamless dynamodDB schema migrations are gonna be interesting
    see https://stackoverflow.com/questions/31301160/change-the-schema-of-a-dynamodb-table-what-is-the-best-recommended-way  # noqa
    """
    log.info("Migrate called")
    for table in table_classes:
        if not table.exists():
            table.create_table(wait=True)
            log.info(f"Migrate created table: {table}")
