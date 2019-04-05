"""
Tests the `blacklight.exporters_alpha_solrmarc` classes.
"""

import pytest


# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/conftest.py:
#    sierra_records_by_recnum_range
# django/sierra/blacklight/tests/conftest.py
#    export_to_solr

pytestmark = pytest.mark.django_db

@pytest.fixture
def record_sets(sierra_records_by_recnum_range):
    return {
        'bib_set': sierra_records_by_recnum_range('b4371446')
    }


# TESTS

@pytest.mark.parametrize('export_id, recset_id, core_config, do_delete', [
    ('BibsToAlphaSolrmarc', 'bib_set', {
        'alpha-solrmarc': { 'is_deleted': True }
    }, True),
])
def test_export_process(export_id, recset_id, core_config, record_sets,
                        do_delete, export_to_solr):
    """
    This is a very basic sanity test to make sure that exporters load
    and delete from the correct Solr core.
    """
    record_set = record_sets[recset_id]
    cores = core_config.keys()
    results = export_to_solr(cores, record_set, export_id, do_delete)
    for core, core_results in results.items():
        assert len(core_results['pre']) == 0
        assert len(core_results['load']) > 0
        if core_config[core]['is_deleted']:
            assert len(core_results['del']) == 0
        else:
            assert len(core_results['del']) > 0
