"""
Tests classes derived from `export.exporter.Exporter`.
"""

import pytest


# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/base/tests/conftest.py:
#    sierra_records_by_recnum_range, sierra_full_object_set,
#    new_exporter, get_records, export_records, delete_records,
#    solr_conn, solr_search

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
            'try_delete': ['bibdata', 'marc']
        },
        'EResourcesToSolr': {
            'record_set': eres_set,
            'cores': ['haystack'],
            'try_delete': ['haystack']
        },
        'ItemsToSolr': {
            'record_set': item_set,
            'cores': ['haystack'],
            'try_delete': ['haystack']
        },
        'ItemStatusesToSolr': {
            'record_set': istatus_set,
            'cores': ['haystack'],
            'try_delete': []
        },
        'ItypesToSolr': {
            'record_set': itype_set,
            'cores': ['haystack'],
            'try_delete': []
        },
        'LocationsToSolr': {
            'record_set': location_set,
            'cores': ['haystack'],
            'try_delete': []
        },
        'BibsAndAttachedToSolr': {
            'record_set': bib_set,
            'cores': ['bibdata', 'haystack', 'marc'],
            'try_delete': ['bibdata', 'marc']
        }
    }


# TESTS

@pytest.mark.parametrize('etype_code', [
    'BibsToSolr',
    'EResourcesToSolr',
    'ItemsToSolr',
    'ItemStatusesToSolr',
    'ItypesToSolr',
    'LocationsToSolr',
    'BibsAndAttachedToSolr'])
def test_export_get_records(etype_code, solr_exporter_test_params,
                            new_exporter, get_records):
    """
    For Exporter classes that that get data from Sierra, blah
    """
    exporter = new_exporter(etype_code, 'full_export', 'waiting')
    db_records = get_records(exporter)
    expected_records = solr_exporter_test_params[etype_code]['record_set']
    assert len(db_records) > 0
    assert all([rec in db_records for rec in expected_records])


@pytest.mark.parametrize('etype_code', [
    'BibsToSolr',
    'EResourcesToSolr',
    'ItemsToSolr',
    'ItemStatusesToSolr',
    'ItypesToSolr',
    'LocationsToSolr',
    'BibsAndAttachedToSolr'])
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
        del_results = {c: solr_search(conns[c], {'q': '*'})
                       for c in try_delete}

    for core in cores:
        assert len(pre_results[core]) == 0
        assert len(load_results[core]) > 0
        if core in try_delete:
            assert len(del_results[core]) == 0
