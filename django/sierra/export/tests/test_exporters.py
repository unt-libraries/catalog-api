"""
Tests classes derived from `export.exporter.Exporter`.
"""

import pytest


# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/base/tests/conftest.py:
#    records_by_recnum_range, new_exporter, export_records,
#    delete_records, solr_conn, solr_search

pytestmark = pytest.mark.django_db


@pytest.fixture
def solr_exporter_test_params(records_by_recnum_range):
    bib_set = records_by_recnum_range('b4371446')
    return {
        'BibsToSolr': {
            'record_set': bib_set,
            'core_tests': {
                'bibdata': { 'query': '*', 'result': 1 },
                'marc': { 'query': '*', 'result': 1 }
            }
        },
    }


# TESTS

@pytest.mark.parametrize('etype_code', ['BibsToSolr'])
def test_exports_to_solr(etype_code, solr_exporter_test_params, new_exporter,
                         export_records, delete_records, solr_conn,
                         solr_search):
    """
    For Exporter classes that load data into Solr, blah
    """
    record_set = solr_exporter_test_params[etype_code]['record_set']
    core_tests = solr_exporter_test_params[etype_code]['core_tests']
    cores = core_tests.keys()
    load_exporter = new_exporter(etype_code, 'full_export', 'waiting')
    conns = {c: solr_conn(c) for c in cores}
    pre_results = {c: solr_search(conns[c], {'q': core_tests[c]['query']})
                   for c in cores}
    export_records(load_exporter, record_set )
    load_results = {c: solr_search(conns[c], {'q': core_tests[c]['query']})
                    for c in cores}
    del_exporter = new_exporter(etype_code, 'full_export', 'waiting')
    delete_records(del_exporter, record_set)
    del_results = {c: solr_search(conns[c], {'q': core_tests[c]['query']})
                   for c in cores}

    for core in cores:
        assert len(pre_results[core]) == 0
        assert len(load_results[core]) == core_tests[core]['result']
        assert len(del_results[core]) == 0
