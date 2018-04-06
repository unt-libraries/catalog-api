"""
Tests catalog-api database routers.

The default Django DB ("default") should contain data for all apps
except the "base" app. Base app models model Sierra data and should
pull from the Sierra DB ("sierra"). No tables should be present in both
databases.
"""

import pytest
from model_mommy import mommy

from django.db import connections, DatabaseError
from django.apps import apps
from django.conf import settings

from sierra import routers
from export import models as em
from base import models as bm


pytestmark = pytest.mark.django_db


SIERRA_TEST_MODEL_CLASS = apps.get_app_config('base').models.values()[0]
EXPORT_TEST_MODEL_CLASS = apps.get_app_config('export').models.values()[0]


@pytest.mark.parametrize('model_class, database, op, expected', [
    (EXPORT_TEST_MODEL_CLASS, 'default', 'read', True),
    (SIERRA_TEST_MODEL_CLASS, 'sierra', 'read', True),
    (EXPORT_TEST_MODEL_CLASS, 'sierra', 'read', False),
    (SIERRA_TEST_MODEL_CLASS, 'default', 'read', False),
    (EXPORT_TEST_MODEL_CLASS, 'default', 'write', True),
    (SIERRA_TEST_MODEL_CLASS, 'sierra', 'write', DatabaseError),
    (SIERRA_TEST_MODEL_CLASS, 'default', 'write', DatabaseError),
])
def test_sierra_router_db_for_op(model_class, database, op, expected):
    """
    Test that SierraRouter.db_for_{op} (db_for_read or db_for_write)
    gives the expected result for the given model_class and database
    """
    test_method = getattr(routers.SierraRouter, 'db_for_{}'.format(op))
    router, model = routers.SierraRouter(), model_class()
    try:
        result = test_method(router, model) == database
    except Exception as e:
        result = type(e)
    assert result == expected


@pytest.mark.parametrize('model_class, database, testing, expected', [
    (EXPORT_TEST_MODEL_CLASS, 'default', False, True),
    (EXPORT_TEST_MODEL_CLASS, 'default', True, True),
    (EXPORT_TEST_MODEL_CLASS, 'sierra', False, False),
    (EXPORT_TEST_MODEL_CLASS, 'sierra', True, False),
    (SIERRA_TEST_MODEL_CLASS, 'sierra', False, False),
    (SIERRA_TEST_MODEL_CLASS, 'sierra', True, True),
    (SIERRA_TEST_MODEL_CLASS, 'default', False, False),
    (SIERRA_TEST_MODEL_CLASS, 'default', True, False),
])
def test_sierra_router_allow_migrate(model_class, database, testing, expected):
    """
    Test that SierraRouter.allow_migrate returns the expected result
    for the given model_class and database, depending on whether or not
    it's a testing environment
    """
    router, app_label = routers.SierraRouter(), model_class()
    settings.TESTING = testing
    result = router.allow_migrate(database, app_label)
    settings.TESTING = True
    assert result == expected


@pytest.mark.parametrize('model_class, database, expected', [
    (EXPORT_TEST_MODEL_CLASS, 'default', True),
    (SIERRA_TEST_MODEL_CLASS, 'sierra', True),
    (EXPORT_TEST_MODEL_CLASS, 'sierra', False),
    (SIERRA_TEST_MODEL_CLASS, 'default', False),
])
def test_table_in_database(model_class, database, expected):
    """
    Test that the given model_class's table is (or isn't) in the given
    database
    """
    cursor = connections[database].cursor()
    tables = cursor.db.introspection.django_table_names(only_existing=True)
    assert (model_class._meta.db_table in tables) == expected


@pytest.mark.parametrize('model_class, database', [
    (EXPORT_TEST_MODEL_CLASS, 'default'),
    (SIERRA_TEST_MODEL_CLASS, 'sierra'),
])
def test_db_read_routing(model_class, database):
    """
    Test that the given model_class actually reads from the given
    database
    """
    queryset = model_class.objects.all()
    assert queryset.db == database


# The next test only tests that the EXPORT_TEST_MODEL_CLASS writes to
# the default database. It doesn't test SIERRA_TEST_MODEL_CLASS because
# the save method on the base.models.ReadOnly model class doesn't allow
# saving, so saving fails before the router even comes into play. I
# couldn't figure out a circumstance in a realistic setting where the
# save method on the model would be bypassed, at least not one that's
# easily testable.
def test_db_write_routing():
    """
    Test that the given model_class actually writes to the given
    database
    """
    instance = mommy.make(EXPORT_TEST_MODEL_CLASS)
    queryset = EXPORT_TEST_MODEL_CLASS.objects.filter(pk=instance.pk)
    does_write = len(queryset) == 1 and queryset.db == 'default'
    assert does_write
