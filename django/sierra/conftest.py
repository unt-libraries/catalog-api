"""
Contains all shared pytest fixtures
"""

import pytest
import pysolr
from model_mommy import mommy

from django.conf import settings

import utils
from export import models as em
from base import models as bm


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
def new_export_instance(export_type, export_filter, status):
    def _new_export_instance(et_code, ef_code, st_code):
        return mommy.make(
            em.ExportInstance,
            status=status(st_code),
            export_type=export_type(et_code),
            export_filter=export_filter(ef_code),
            errors=0,
            warnings=0
        )
    return _new_export_instance


@pytest.fixture
def new_exporter(new_export_instance):
    def _new_exporter(et_code, ef_code, st_code, options={}):
        instance = new_export_instance(et_code, ef_code, st_code)
        exp_class = utils.load_class(instance.export_type.path)
        return exp_class(instance.pk, ef_code, et_code, options)
    yield _new_exporter
    em.ExportInstance.objects.all().delete()


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


# Utils-related fixtures

@pytest.fixture
def solr_conn():
    url = 'http://{}:{}/solr'.format(settings.SOLR_HOST, settings.SOLR_PORT)
    def _solr_conn(core_name):
        connection = pysolr.Solr('{}/{}'.format(url, core_name))
        _solr_conn.connections.append(connection)
        return connection
    _solr_conn.connections = []
    yield _solr_conn
    for conn in _solr_conn.connections:
        conn.delete(q='*:*')


@pytest.fixture
def solr_search():
    def _solr_search(conn, options):
        results = conn.search(**options)
        return [r for r in results]
    return _solr_search
