"""
Tests classes derived from `export.exporter.Exporter`.
"""

import pytest

from django.conf import settings

# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/base/tests/conftest.py:
#    sierra_records_by_recnum_range, sierra_full_object_set,
#    new_exporter, get_records, export_records, delete_records,
#    solr_conn, solr_search, export_type,
#    configure_export_type_classpaths, assert_records_are_indexed

pytestmark = pytest.mark.django_db


@pytest.fixture(scope='module')
def do_export_type_cfg(configure_export_type_classpaths):
    mapping = {'ItemsToSolr': 'export.basic_exporters.ItemsToSolr'}
    configure_export_type_classpaths(mapping)


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
            'record_set': bib_set
        },
        'EResourcesToSolr': {
            'export_type': 'EResourcesToSolr',
            'record_set': eres_set
        },
        'ItemsToSolr': {
            'export_type': 'ItemsToSolr',
            'record_set': item_set
        },
        'ItemStatusesToSolr': {
            'export_type': 'ItemStatusesToSolr',
            'record_set': istatus_set
        },
        'ItypesToSolr': {
            'export_type': 'ItypesToSolr',
            'record_set': itype_set
        },
        'LocationsToSolr': {
            'export_type': 'LocationsToSolr',
            'record_set': location_set
        },
        'BibsAndAttachedToSolr: Plain Bib Only': {
            'export_type': 'BibsAndAttachedToSolr',
            'record_set': bib_set
        },
        'BibsAndAttachedToSolr: EResource Bib (with holdings)': {
            'export_type': 'BibsAndAttachedToSolr',
            'record_set': er_bib_set
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
                            new_exporter, get_records, do_export_type_cfg):
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
                         solr_search, assert_records_are_indexed,
                         do_export_type_cfg):
    """
    For Exporter classes that load data into Solr, blah
    """
    # Get basic test parameters from the `solr_exporter_test_params`
    # fixture.
    test_params = solr_exporter_test_params[test_id]
    export_type_name = test_params['export_type']
    input_record_set = test_params['record_set']

    # Set up parameters defining the export job by creating and
    # inspecting the main (parent) exporter object.
    parent = new_exporter(export_type_name, 'full_export', 'waiting')
    parent_indexes = getattr(parent, 'indexes', {}).values()
    children_dict = getattr(parent, 'exporters', {})
    child_indexes = [i for c in children_dict.values()
                     for i in getattr(c, 'indexes', {}).values()]

    # Capture results for all relevant Solr cores BEFORE the test, for
    # comparison. (To make sure Solr indexes start out empty.)
    conn_names = set(getattr(i, 'using', 'default')
                     for i in parent_indexes + child_indexes)
    pre_results = {c: solr_search(solr_conn(c), '*') for c in conn_names}

    # Run the test by running the parent exporter `export_records` job.
    export_records(parent, input_record_set)

    # Capture results for all relevant Solr cores after the test.
    results = {c: solr_search(solr_conn(c), '*') for c in conn_names}

    # Check results.
    for index in parent_indexes:
        assert len(pre_results[index.using]) == 0
        assert_records_are_indexed(index, input_record_set,
                                   results[index.using])

    for child_name, child in children_dict.items():
        for index in getattr(child, 'indexes', {}).values():
            assert len(pre_results[index.using]) == 0
            try:
                child_rset = [
                    cr for pr in input_record_set
                        for cr in parent.get_attached_records(pr)[child_name]
                    ]
            except KeyError:
                child_rset = input_record_set
            assert_records_are_indexed(index, child_rset, results[index.using])

    if parent.get_deletions() is not None:
        del_exporter = new_exporter(export_type_name, 'full_export', 'waiting')
        delete_records(del_exporter, input_record_set)
        for index in parent_indexes:
            assert len(solr_search(solr_conn(index.using), '*')) == 0

