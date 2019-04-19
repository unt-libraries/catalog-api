"""
Tests classes derived from `export.exporter.Exporter`.
"""

import pytest


# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/base/tests/conftest.py:
#    sierra_records_by_recnum_range, sierra_full_object_set,
#    new_exporter, export_records, delete_records, solr_conn,
#    solr_search

pytestmark = pytest.mark.django_db


@pytest.fixture
def solr_exporter_test_params(sierra_records_by_recnum_range,
                              sierra_full_object_set):
    bib_set = sierra_records_by_recnum_range('b4371446')
    eres_set = sierra_records_by_recnum_range('e1001249')
    item_set = sierra_records_by_recnum_range('i4264281')
    itype_set = sierra_full_object_set('ItypeProperty')
    istatus_set = sierra_full_object_set('ItemStatusProperty')
    location_set = sierra_full_object_set('Location')

    return {
        'BibsToSolr': {
            'record_set': bib_set,
            'cores': ['bibdata', 'marc'],
            'try_delete': True
        },
        'EResourcesToSolr': {
            'record_set': eres_set,
            'cores': ['haystack'],
            'try_delete': True
        },
        'ItemsToSolr': {
            'record_set': item_set,
            'cores': ['haystack'],
            'try_delete': True
        },
        'ItemStatusesToSolr': {
            'record_set': istatus_set,
            'cores': ['haystack'],
            'try_delete': False
        },
        'ItypesToSolr': {
            'record_set': itype_set,
            'cores': ['haystack'],
            'try_delete': False
        },
        'LocationsToSolr': {
            'record_set': location_set,
            'cores': ['haystack'],
            'try_delete': False
        },
    }


# TESTS

@pytest.mark.parametrize('etype_code', [
    'BibsToSolr',
    'EResourcesToSolr',
    'ItemsToSolr',
    'ItemStatusesToSolr',
    'ItypesToSolr',
    'LocationsToSolr'])
def test_exports_to_solr(etype_code, solr_exporter_test_params, new_exporter,
                         export_records, delete_records, solr_conn,
                         solr_search):
    """
    For Exporter classes that load data into Solr, blah
    """
    record_set = solr_exporter_test_params[etype_code]['record_set']
    cores = solr_exporter_test_params[etype_code]['cores']
    try_delete = solr_exporter_test_params[etype_code]['try_delete']
    load_exporter = new_exporter(etype_code, 'full_export', 'waiting')
    conns = {c: solr_conn(c) for c in cores}
    pre_results = {c: solr_search(conns[c], {'q': '*'}) for c in cores}
    export_records(load_exporter, record_set)
    load_results = {c: solr_search(conns[c], {'q': '*'}) for c in cores}
    del_results = {}
    if try_delete:
        del_exporter = new_exporter(etype_code, 'full_export', 'waiting')
        delete_records(del_exporter, record_set)
        del_results = {c: solr_search(conns[c], {'q': '*'}) for c in cores}

    for core in cores:
        assert len(pre_results[core]) == 0
        assert len(load_results[core]) > 0
        if try_delete:
            assert len(del_results[core]) == 0


@pytest.mark.parametrize('etype_code', [
    'BibsToSolr',
    'EResourcesToSolr',
    'ItemsToSolr',
    'ItemStatusesToSolr',
    'ItypesToSolr',
    'LocationsToSolr'])
def test_max_chunk_settings_overrides(etype_code, settings, new_exporter):
    """
    Using EXPORTER_MAX_RC_CONFIG and EXPORTER_MAX_DC_CONFIG settings
    should override values set on the class when an exporter is
    instantiated.
    """
    new_rc_val, new_dc_val = 77777, 88888
    settings.EXPORTER_MAX_RC_CONFIG[etype_code] = new_rc_val
    settings.EXPORTER_MAX_DC_CONFIG[etype_code] = new_dc_val
    load_exporter = new_exporter(etype_code, 'full_export', 'waiting')
    assert load_exporter.max_rec_chunk == new_rc_val
    assert load_exporter.max_del_chunk == new_dc_val
    assert new_rc_val != type(load_exporter).max_rec_chunk
    assert new_dc_val != type(load_exporter).max_del_chunk


@pytest.mark.parametrize('etype_code', [
    'BibsToSolr',
    'EResourcesToSolr',
    'ItemsToSolr',
    'ItemStatusesToSolr',
    'ItypesToSolr',
    'LocationsToSolr'])
def test_max_chunk_settings_defaults(etype_code, settings, new_exporter):
    """
    If NOT using EXPORTER_MAX_RC_CONFIG and EXPORTER_MAX_DC_CONFIG
    settings, the `max_rec_chunk` and `max_del_chunk` values for a
    given job should come from the exporter class.
    """
    settings.EXPORTER_MAX_RC_CONFIG = {}
    settings.EXPORTER_MAX_DC_CONFIG = {}
    load_exporter = new_exporter(etype_code, 'full_export', 'waiting')
    assert load_exporter.max_rec_chunk == type(load_exporter).max_rec_chunk
    assert load_exporter.max_del_chunk == type(load_exporter).max_del_chunk

