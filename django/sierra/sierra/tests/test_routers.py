"""
Tests catalog-api database routers.

The default Django DB ("default") should contain data for all apps
except the "base" app. Base app models model Sierra data and should
pull from the Sierra DB ("sierra"). No tables should be present in both
databases.
"""

import pytest

from django.db import connections, DatabaseError

from sierra import routers
from export import models as em
from base import models as bm


# FIXTURES AND TEST DATA
# External fixtures used below can be found in
# django/sierra/conftest.py:
#    make_model_instance
# 
# The `settings` fixture used in a few tests is built into
# pytest-django.

pytestmark = pytest.mark.django_db


SIERRA_TEST_MODEL_CLASS = bm.FixfldTypeMyuser
EXPORT_TEST_MODEL_CLASS = em.Status


# TESTS

@pytest.mark.parametrize('model_class, op, testing, expected', [
    (EXPORT_TEST_MODEL_CLASS, 'read', True, 'default'),
    (EXPORT_TEST_MODEL_CLASS, 'read', False, 'default'),
    (SIERRA_TEST_MODEL_CLASS, 'read', True, 'sierra'),
    (SIERRA_TEST_MODEL_CLASS, 'read', False, 'sierra'),
    (EXPORT_TEST_MODEL_CLASS, 'write', True, 'default'),
    (EXPORT_TEST_MODEL_CLASS, 'write', False, 'default'),
    (SIERRA_TEST_MODEL_CLASS, 'write', True, 'sierra'),
    (SIERRA_TEST_MODEL_CLASS, 'write', False, DatabaseError),
])
def test_sierra_router_db_for_op(model_class, op, testing, expected, settings):
    """
    Test that SierraRouter.db_for_{op} (db_for_read or db_for_write)
    returns the expected result for the given model_class.
    """
    test_method = getattr(routers.SierraRouter, 'db_for_{}'.format(op))
    router, model = routers.SierraRouter(), model_class()
    settings.TESTING = testing
    try:
        result = test_method(router, model)
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
def test_sierra_router_allow_migrate(model_class, database, testing, expected,
                                     settings):
    """
    Test that SierraRouter.allow_migrate returns the expected result
    for the given model_class and database, depending on whether or not
    it's a testing environment.
    """
    router, app_label = routers.SierraRouter(), model_class()._meta.app_label
    settings.TESTING = testing
    result = router.allow_migrate(database, app_label)
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
    database.
    """
    cursor = connections[database].cursor()
    tables = cursor.db.introspection.django_table_names(only_existing=True)
    assert (model_class._meta.db_table in tables) == expected


@pytest.mark.parametrize('model_class, testing, expected', [
    (EXPORT_TEST_MODEL_CLASS, True, 'default'),
    (EXPORT_TEST_MODEL_CLASS, False, 'default'),
    (SIERRA_TEST_MODEL_CLASS, True, 'sierra'),
    (SIERRA_TEST_MODEL_CLASS, False, 'sierra'),
])
def test_db_read_routing(model_class, testing, expected, settings):
    """
    Test that the given model_class actually reads from the given
    database.
    """
    settings.TESTING = testing
    queryset = model_class.objects.all()
    assert queryset.db == expected


@pytest.mark.parametrize('model_class, testing, expected', [
    (EXPORT_TEST_MODEL_CLASS, True, 'default'),
    (EXPORT_TEST_MODEL_CLASS, False, 'default'),
    (SIERRA_TEST_MODEL_CLASS, True, 'sierra'),
    (SIERRA_TEST_MODEL_CLASS, False, DatabaseError),
])
def test_db_write_routing(model_class, testing, expected, settings,
                          make_model_instance):
    """
    Test that the given model_class actually writes to the given
    database, or not.
    """
    settings.TESTING = testing
    try:
        instance = make_model_instance(model_class)
    except Exception as e:
        if type(e) == expected:
            result = True
        else:
            raise
    else:
        queryset = model_class.objects.filter(pk=instance.pk)
        result = len(queryset) == 1 and queryset.db == expected
    assert result
