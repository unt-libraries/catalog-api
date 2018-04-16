"""
Contains pytest fixtures shared by Catalog API blacklight app
"""

import pytest

# External fixtures used below can be found in
# django/sierra/base/tests/conftest.py:
#    new_exporter, export_records, delete_records, solr_conn,
#    solr_search

@pytest.fixture
def export_to_solr(new_exporter, export_records, delete_records, solr_conn,
                       solr_search):
    """
    Export test records to Solr and return results.

    This is a pytest fixture that allows you to run a set of test
    records (`recset`) through an export process (`etype_code`) and
    then pull results from a particular Solr `core`. If `delete` is
    True, then it will attempt to delete the test records as well.

    Returns a dictionary containing results at three different states.
    `pre` contains results before the export; `load` contains results
    after the export; `del` contains results after the deletion, or
    `None` if `delete` is False.
    """
    def _export_to_solr(core, recset, etype_code, delete=True):
        exp = new_exporter(etype_code, 'full_export', 'waiting')
        conn = solr_conn(core)
        pre_results = solr_search(conn, {'q': '*'})
        export_records(exp, recset)
        load_results = solr_search(conn, {'q': '*'})
        del_results = None
        if delete:
            del_exp = new_exporter(etype_code, 'full_export', 'waiting')
            del_recset = [r.record_metadata for r in recset]
            delete_records(del_exp, del_recset)
            del_results = solr_search(conn, {'q': '*'})

        return {'pre': pre_results, 'load': load_results, 'del': del_results}
    return _export_to_solr
