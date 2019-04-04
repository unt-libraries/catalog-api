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

# TESTS

def test_bibs_to_alpha_solrmarc(sierra_records_by_recnum_range,
                                export_to_solr):
    """
    This is a very basic sanity test to make sure that the
    BibsToAlphaSolrmarc exporter loads and deletes from the correct
    Solr core.
    """
    cores = ['alpha-solrmarc']
    record_set = sierra_records_by_recnum_range('b4371446')
    results = export_to_solr(cores, record_set, 'BibsToAlphaSolrmarc')
    for core in cores:
        assert len(results['pre'][core]) == 0
        assert len(results['load'][core]) > 0
        assert len(results['del'][core]) == 0
