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
    then pull results from one or more Solr `cores`.

    If `do_delete` is True, then it attempts to run a `delete_records`
    process after the load process.

    Returns a dictionary containing results at three different states.
    `pre` contains results before the export; `load` contains results
    after the export; `del` contains results after the deletion.
    """
    def _export_to_solr(cores, recset, etype_code, do_delete=True):
        conns, results = {}, {}
        for core in cores:
            conns[core] = solr_conn(core)
            results[core] = {'pre': solr_search(conns[core], {'q': '*'})}

        exp = new_exporter(etype_code, 'full_export', 'waiting')
        export_records(exp, recset)

        for core in cores:
            results[core]['load'] = solr_search(conns[core], {'q': '*'})

        if do_delete:
            del_exp = new_exporter(etype_code, 'full_export', 'waiting')
            del_recset = [r.record_metadata for r in recset]
            delete_records(del_exp, del_recset)

        for core in cores:
            results[core]['del'] = solr_search(conns[core], {'q': '*'})

        return results
    return _export_to_solr
