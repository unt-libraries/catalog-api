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
    er_bib_set = sierra_records_by_recnum_range('b5784429')
    eres_set = sierra_records_by_recnum_range('e1001249')
    item_set = sierra_records_by_recnum_range('i4264281')
    itype_set = sierra_full_object_set('ItypeProperty')
    istatus_set = sierra_full_object_set('ItemStatusProperty')
    location_set = sierra_full_object_set('Location')

    return {
        'BibsToSolr': {
            'export_type': 'BibsToSolr',
            'record_set': bib_set,
            'cores': ['bibdata', 'marc'],
            'try_delete': ['bibdata', 'marc']
        },
        'EResourcesToSolr': {
            'export_type': 'EResourcesToSolr',
            'record_set': eres_set,
            'cores': ['haystack'],
            'try_delete': ['haystack']
        },
        'ItemsToSolr': {
            'export_type': 'ItemsToSolr',
            'record_set': item_set,
            'cores': ['haystack'],
            'try_delete': ['haystack']
        },
        'ItemStatusesToSolr': {
            'export_type': 'ItemStatusesToSolr',
            'record_set': istatus_set,
            'cores': ['haystack'],
            'try_delete': []
        },
        'ItypesToSolr': {
            'export_type': 'ItypesToSolr',
            'record_set': itype_set,
            'cores': ['haystack'],
            'try_delete': []
        },
        'LocationsToSolr': {
            'export_type': 'LocationsToSolr',
            'record_set': location_set,
            'cores': ['haystack'],
            'try_delete': []
        },
        'BibsAndAttachedToSolr: Plain Bib Only': {
            'export_type': 'BibsAndAttachedToSolr',
            'record_set': bib_set,
            'cores': ['bibdata', 'haystack', 'marc'],
            'try_delete': ['bibdata', 'marc']
        },
        'BibsAndAttachedToSolr: EResource Bib (with holdings)': {
            'export_type': 'BibsAndAttachedToSolr',
            'record_set': er_bib_set,
            'cores': ['bibdata', 'haystack', 'marc'],
            'try_delete': ['bibdata', 'marc']
        }
    }


# TESTS

@pytest.mark.parametrize('test_id', [
    'BibsToSolr',
    'EResourcesToSolr',
    'ItemsToSolr',
    'ItemStatusesToSolr',
    'ItypesToSolr',
    'LocationsToSolr',
    'BibsAndAttachedToSolr: Plain Bib Only',
    'BibsAndAttachedToSolr: EResource Bib (with holdings)'])
def test_export_get_records(test_id, solr_exporter_test_params,
                            new_exporter, get_records):
    """
    For Exporter classes that that get data from Sierra, blah
    """
    test_params = solr_exporter_test_params[test_id]
    export_type = test_params['export_type']
    exporter = new_exporter(export_type, 'full_export', 'waiting')
    db_records = get_records(exporter)
    expected_records = test_params['record_set']
    assert len(db_records) > 0
    assert all([rec in db_records for rec in expected_records])


@pytest.mark.parametrize('test_id', [
    'BibsToSolr',
    'EResourcesToSolr',
    'ItemsToSolr',
    'ItemStatusesToSolr',
    'ItypesToSolr',
    'LocationsToSolr',
    'BibsAndAttachedToSolr: Plain Bib Only',
    'BibsAndAttachedToSolr: EResource Bib (with holdings)'])
def test_exports_to_solr(test_id, solr_exporter_test_params, new_exporter,
                         export_records, delete_records, solr_conn,
                         solr_search):
    """
    For Exporter classes that load data into Solr, blah
    """
    test_params = solr_exporter_test_params[test_id]
    export_type = test_params['export_type']
    record_set = test_params['record_set']
    cores = test_params['cores']
    try_delete = test_params['try_delete']
    load_exporter = new_exporter(export_type, 'full_export', 'waiting')
    conns = {c: solr_conn(c) for c in cores}
    pre_results = {c: solr_search(conns[c], '*') for c in cores}
    export_records(load_exporter, record_set)
    load_results = {c: solr_search(conns[c], '*') for c in cores}
    del_results = {}
    if try_delete:
        del_exporter = new_exporter(export_type, 'full_export', 'waiting')
        delete_records(del_exporter, record_set)
        del_results = {c: solr_search(conns[c], '*')
                       for c in try_delete}

    for core in cores:
        assert len(pre_results[core]) == 0
        assert len(load_results[core]) > 0
        if core in try_delete:
            assert len(del_results[core]) == 0
