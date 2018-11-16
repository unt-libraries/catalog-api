"""
Contains all shared pytest fixtures
"""

import pytest
import pysolr
import datetime
import pytz

from django.contrib.auth.models import User

import utils
from utils.test_helpers import fixture_factories as ff
from export import models as em
from base import models as bm


# General utility fixtures

@pytest.fixture(scope='function')
def model_instance():
    """
    Function-level pytest fixture. Returns a factory object that tracks
    model instances as they're created and clears them out when the
    test completes.
    """
    with ff.FactoryTracker(ff.TestInstanceFactory()) as make:
        yield make


@pytest.fixture(scope='module')
def global_model_instance():
    """
    Module-level pytest fixture. Returns a factory object that tracks
    model instances as they're created and clears them out when the
    module completes. This is the same as `model_instance`, except
    instances persist throughout all tests in a module.
    """
    with ff.FactoryTracker(ff.TestInstanceFactory()) as make:
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
        exp_class = utils.load_class(instance.export_type.path)
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
