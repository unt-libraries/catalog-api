"""
Contains all shared pytest fixtures and hooks
"""

import pytest
import redis
import pysolr
import datetime
import pytz

from django.conf import settings
from django.contrib.auth.models import User

from utils.test_helpers import fixture_factories as ff
from export import models as em
from base import models as bm
from api.models import APIUser


# HOOKS -- These control setup / teardown for all tests.

def pytest_runtest_teardown(item, nextitem):
    """
    Flush the Redis appdata store after every single test. We can get
    away with this for Redis because it's fast. (There is no
    discernable difference in test run time with and without this.)
    """
    conn = redis.StrictRedis(**settings.REDIS_CONNECTION)
    try:
        conn.flushdb()
    except Exception:
        # Occasionally flushdb fails. If it does, just try again.
        conn.flushdb()


# General utility fixtures

@pytest.fixture(scope='module')
def mytempdir(tmpdir_factory):
    """
    Module-level fixture for creating a temporary dir for testing
    purposes.
    """
    return tmpdir_factory.mktemp('data')


@pytest.fixture(scope='module')
def make_tmpfile(mytempdir):
    """
    Module-level factory fixture for creating a data file in a temp
    directory. Pass the `data` to write along with the `filename`;
    returns a full absolute path to the written file.
    """
    def make(data, filename):
        path = mytempdir.join(filename)
        with open(str(path), 'w') as fh:
            fh.write(data)
        return path
    return make


@pytest.fixture(scope='function')
def model_instance():
    """
    Function-level pytest fixture. Returns a factory object that tracks
    model instances as they're created and clears them out when the
    test completes. Instances are created via the manager's `create`
    method.
    """
    with ff.FactoryTracker(ff.TestInstanceFactory()) as make:
        yield make


@pytest.fixture(scope='module')
def global_model_instance(django_db_blocker):
    """
    Module-level pytest fixture. Returns a factory object that tracks
    model instances as they're created and clears them out when the
    module completes. This is the same as `model_instance`, except
    instances persist throughout all tests in a module.
    """
    with django_db_blocker.unblock():
        with ff.FactoryTracker(ff.TestInstanceFactory()) as make:
            yield make


@pytest.fixture(scope='function')
def model_instance_caller():
    """
    Function-level pytest fixture. Returns a factory object that tracks
    model instances as they're created and clears them out when the
    test completes. Instances are created using whatever callable obj
    is passed to the factory.
    """
    with ff.FactoryTracker(ff.TestInstanceCallerFactory()) as make:
        yield make


@pytest.fixture(scope='function')
def installed_test_class():
    """
    Function-level pytest fixture for temporarily installing a test
    class onto a particular module. The class may be installed as a
    brand new attribute, or it may override an existing class. Any
    changes are reverted after the test runs.
    """
    with ff.FactoryTracker(ff.TestClassRegistry()) as make:
        yield make


# Solr-related fixtures

@pytest.fixture(scope='function')
def solr_data_assembler():
    """
    Function-level pytest fixture. Returns a callable factory for
    creating SolrTestDataAssembler objects, which allow you to make and
    manage multiple sets of Solr test data records. It tracks all
    records added via the factory and then clears them out when the
    test completes.
    """
    with ff.FactoryTracker(ff.SolrTestDataAssemblerFactory()) as make:
        yield make


@pytest.fixture(scope='module')
def global_solr_data_assembler():
    """
    Module-level pytest fixture. Returns a callable factory for
    creating SolrTestDataAssembler objects, which allow you to make and
    manage multiple sets of Solr test data records. It tracks all
    records added via the factory and then clears them out when the
    module completes. This is the same as `solr_data_assembler`, except
    that the records created persist throughout all tests in a module.
    """
    with ff.FactoryTracker(ff.SolrTestDataAssemblerFactory()) as make:
        yield make


@pytest.fixture(scope='function')
def solr_conn():
    """
    Function-level pytest fixture. Returns a callable factory object
    for creating a pysolr connection, for interacting with a Solr core.
    The connection is completely cleared (all records within the core
    deleted) when the test completes.
    """
    with ff.FactoryTracker(ff.TestSolrConnectionFactory()) as make:
        yield make


@pytest.fixture(scope='function')
def solr_conns(solr_conn, settings):
    """
    Function-level pytest fixture. Returns a dict of all configured
    Solr connections using the `solr_conn` fixture, to help ensure Solr
    records are cleared when a test finishes or raises an error.
    """
    return {name: solr_conn(name) for name in settings.HAYSTACK_CONNECTIONS}


@pytest.fixture(scope='module')
def global_solr_conn():
    """
    Module-level pytest fixture. Returns a callable factory object for
    creating a pysolr connection, for interacting with a Solr core. The
    connection is completely cleared (all records within the core
    deleted) when the module completes. This is the same as
    `solr_conn`, except that the connection isn't cleared after each
    function.
    """
    with ff.FactoryTracker(ff.TestSolrConnectionFactory()) as make:
        yield make


@pytest.fixture
def solr_search():
    """
    Pytest fixture. Returns a search utility function that queries Solr
    via the given connection using the given query and parameters. It
    fetches all search results from the index and returns them.
    """
    def _solr_search(conn, query, **params):
        results, page, params['start'], params['rows'] = [], True, 0, 100
        while page:
            page = [item for item in conn.search(query, **params)]
            params['start'] += params['rows']
            results.extend(page)
        return results
    return _solr_search


# Base app-related fixtures

@pytest.fixture
def sierra_records_by_recnum_range():
    """
    Return Sierra records by record number range.

    This is a pytest fixture that returns a set of objects based on the
    provided start and (optional) end record number. Uses the
    appropriate model based on the type of record, derived from the
    first character of the record numbers (e.g., b is BibRecord).

    Metadata about the filter used is added to an `info` attribute on
    the object queryset that is returned.
    """
    def _sierra_records_by_recnum_range(start, end=None):
        rectype = start[0]
        filter_options = {'record_range_from': start, 
                          'record_range_to': end or start}
        modelname = bm.RecordMetadata.record_type_models[rectype]
        model = getattr(bm, modelname)
        recset = model.objects.filter_by('record_range', filter_options)
        recset.info = {
            'filter_method': 'filter_by',
            'filter_args': ['record_range', filter_options],
            'filter_kwargs': {},
            'export_filter_type': 'record_range',
            'export_filter_options': filter_options,
        }
        return recset
    return _sierra_records_by_recnum_range


@pytest.fixture
def sierra_full_object_set():
    """
    Return a full set of objects from a Sierra (base) model.

    This is a pytest fixture that returns a full set of model objects
    based on the type of model provided.

    Metadata about the filter used is added to an `info` attribute on
    the object queryset that is returned.
    """
    def _sierra_full_object_set(model_name):
        recset = getattr(bm, model_name).objects.all()
        recset.info = {
            'filter_method': 'all',
            'filter_args': [],
            'filter_kwargs': {},
            'export_filter_type': 'full_export',
            'export_filter_options': {}
        }
        return recset
    return _sierra_full_object_set


# Export app-related fixtures

@pytest.fixture(scope='module')
def configure_export_types(django_db_blocker):
    """
    Module-level pytest fixture for handling ExportType setup and
    teardown. Modify the `path` field (classpath string tying the
    ExportType to the Exporter class that should handle jobs of that
    ET) for a group of ET instances. Provide the list of ET codes or
    names (`et_codes`) and a new `base_path` to use. Original paths are
    restored after all module tests have run.

    Returns a dict mapping ET codes to ET instances (with new
    classpaths).
    """

    path_cache, et_cache = {}, {}

    def _configure_export_types(et_codes, base_path=None):
        etypes = {}
        for et_code in et_codes:
            export_type = em.ExportType.objects.get(code=et_code)
            if base_path and et_code not in path_cache:
                new_path = '{}.{}'.format(base_path, et_code)
                path_cache[et_code] = export_type.path
                export_type.path = new_path
                export_type.save()
            etypes[et_code] = export_type
        et_cache.update(etypes)
        return etypes

    with django_db_blocker.unblock():
        yield _configure_export_types

        for et_code, original_path in path_cache.items():
            et_cache[et_code].path = original_path
            et_cache[et_code].save()


@pytest.fixture
def export_type():
    def _export_type(code):
        return em.ExportType.objects.get(code=code)
    return _export_type


@pytest.fixture
def export_filter():
    def _export_filter(code):
        return em.ExportFilter.objects.get(code=code)
    return _export_filter


@pytest.fixture
def status():
    def _status(code):
        return em.Status.objects.get(code=code)
    return _status


@pytest.fixture
def new_export_instance(export_type, export_filter, status,
                        model_instance):
    def _new_export_instance(et_code, ef_code, st_code):
        try:
            test_user = User.objects.get(username='test')
        except User.DoesNotExist:
            test_user = model_instance(User, 'test', 'test@test.com',
                                            'testpass')
        return model_instance(
            em.ExportInstance,
            user=test_user,
            status=status(st_code),
            export_type=export_type(et_code),
            export_filter=export_filter(ef_code),
            errors=0,
            warnings=0,
            timestamp=datetime.datetime.now(pytz.utc)
        )
    return _new_export_instance


@pytest.fixture
def new_exporter(new_export_instance):
    def _new_exporter(et_code, ef_code, st_code, options={}):
        instance = new_export_instance(et_code, ef_code, st_code)
        exp_class = instance.export_type.get_exporter_class()
        return exp_class(instance.pk, ef_code, et_code, options)
    return _new_exporter


@pytest.fixture
def get_records():
    def _get_records(exporter):
        return exporter.get_records()
    return _get_records


@pytest.fixture
def export_records():
    def _export_records(exporter, records):
        exporter.export_records(records)
        exporter.final_callback()
    return _export_records


@pytest.fixture
def delete_records():
    def _delete_records(exporter, records):
        exporter.delete_records(records)
        exporter.final_callback()
    return _delete_records


@pytest.fixture
def get_records_from_index(solr_conns, solr_search):
    """
    Pytest fixture that resturns a test helper function for getting
    records from the provided `record_set` from the provided `index`
    object.
    """
    def _get_records_from_index(index, record_set):
        id_fname = index.reserved_fields['haystack_id']
        meta = index.get_model()._meta
        conn = solr_conns[getattr(index, 'using', 'default')]
        results = solr_search(conn, '*')

        found_records = {}
        for record in record_set:
            cmp_id = '{}.{}.{}'.format(meta.app_label, meta.model_name,
                                       record.pk)
            matches = [r for r in results if r[id_fname] == cmp_id]
            if len(matches) == 1:
                found_records[record.pk] = matches[0]
        return found_records
    return _get_records_from_index


@pytest.fixture
def assert_records_are_indexed(get_records_from_index):
    """
    Pytest fixture that returns a test helper function for checking
    that the provided `record_set` has been indexed by the provided
    `index` object appropriately.
    """
    def _assert_records_are_indexed(index, record_set):
        results = get_records_from_index(index, record_set)
        for record in record_set:
            result = results[record.pk]
            for field in result.keys():
                schema_field = index.get_schema_field(field)
                assert schema_field is not None
                assert schema_field['stored']
    return _assert_records_are_indexed


@pytest.fixture
def assert_records_are_not_indexed(get_records_from_index):
    """
    Pytest fixture that returns a test helper function for checking
    that records in the provided `record_set` are NOT indexed in the
    Solr index represented by the provided `index` object.
    """
    def _assert_records_are_not_indexed(index, record_set):
        assert not get_records_from_index(index, record_set)
    return _assert_records_are_not_indexed


@pytest.fixture
def assert_all_exported_records_are_indexed(assert_records_are_indexed):
    """
    Pytest fixture that returns a test helper function for checking
    that the provided `record_set` has been indexed correctly as a
    result of the provided `exporter` class running its
    `export_records` method.
    """
    def _assert_all_exported_records_are_indexed(exporter, record_set):
        # Check results in all indexes for the parent test_exporter.
        test_indexes = getattr(exporter, 'indexes', {}).values()
        for index in test_indexes:
            assert_records_are_indexed(index, record_set)
        # Check results in all child indexes (if any).
        if hasattr(exporter, 'children'):
            child_rsets = exporter.generate_record_sets(record_set)
            for child_name, child in exporter.children.items():
                child_indexes = getattr(child, 'indexes', {}).values()
                for index in child_indexes:
                    assert_records_are_indexed(index, child_rsets[child_name])
    return _assert_all_exported_records_are_indexed


@pytest.fixture
def assert_deleted_records_are_not_indexed(assert_records_are_not_indexed):
    """
    Pytest fixture that returns a test helper function for checking
    that records in the provided `record_set` have been removed from
    the appropriate indexes as the result of the provided `exporter`
    class running its `delete_records` method.
    """
    def _assert_deleted_records_are_not_indexed(exporter, record_set):
        # If the parent exporter has indexes, check them.
        indexes = getattr(exporter, 'indexes', {}).values()
        # Otherwise, if the parent has a `main_child` that has indexes,
        # then those are the ones to check.
        if not indexes and hasattr(exporter, 'main_child'):
            indexes = getattr(exporter.main_child, 'indexes', {}).values()

        for index in indexes:
            assert_records_are_not_indexed(index, record_set)
    return _assert_deleted_records_are_not_indexed


# API App related fixtures

@pytest.fixture(scope='function')
def apiuser_with_custom_defaults():
    """
    Function-level pytest fixture; returns a function to use for
    updating the APIUser class with custom default permissions.
    Restores the original defaults after the test runs.
    """
    def _apiuser_with_custom_defaults(defaults=None):
        defaults = defaults or {'test_create': False, 'test_update': False,
                                'test_delete': False}
        APIUser.permission_defaults = defaults.copy()
        return APIUser

    old_defaults = APIUser.permission_defaults.copy()
    yield _apiuser_with_custom_defaults
    APIUser.permission_defaults = old_defaults
