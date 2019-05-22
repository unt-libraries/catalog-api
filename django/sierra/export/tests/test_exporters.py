"""
Tests classes derived from `export.exporter.Exporter`.
"""

import pytest

# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/base/tests/conftest.py:
#    sierra_records_by_recnum_range, sierra_full_object_set,
#    record_sets, new_exporter, redis_obj
#    solr_assemble_specific_record_data,
#    setattr_model_instance, derive_exporter_class,
#    assert_all_exported_records_are_indexed,
#    assert_deleted_records_are_not_indexed,
#    assert_records_are_indexed, assert_records_are_not_indexed

pytestmark = pytest.mark.django_db


@pytest.fixture
def basic_exporter_class(derive_exporter_class):
    def _basic_exporter_class(name):
        return derive_exporter_class(name, 'export.basic_exporters')
    return _basic_exporter_class


@pytest.fixture
def batch_exporter_class(derive_exporter_class):
    def _batch_exporter_class(name):
        return derive_exporter_class(name, 'export.batch_exporters')
    return _batch_exporter_class


# TESTS

@pytest.mark.parametrize('et_code, category', [
    ('BibsToSolr', 'basic'),
    ('EResourcesToSolr', 'basic'),
    ('ItemsToSolr', 'basic'),
    ('ItemStatusesToSolr', 'basic'),
    ('ItypesToSolr', 'basic'),
    ('LocationsToSolr', 'basic'),
    ('ItemsBibsToSolr', 'basic'),
    ('BibsAndAttachedToSolr', 'basic'),
    ('AllMetadataToSolr', 'batch'),
])
def test_exporter_class_versions(et_code, category, new_exporter,
                                 basic_exporter_class, batch_exporter_class):
    """
    For all exporter types / classes that are under test in this test
    module, what we get from the `basic_exporter_class` and
    `batch_exporter_class` fixtures should be derived from the `export`
    app.
    """
    if category == 'basic':
        expclass = basic_exporter_class(et_code)
    else:
        expclass = batch_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    assert exporter.app_name == 'export'
    for child_etcode, child in getattr(exporter, 'children', {}).items():
        assert child.app_name == 'export'


@pytest.mark.exports
@pytest.mark.get_records
@pytest.mark.basic
@pytest.mark.parametrize('et_code, rset_code', [
    ('BibsToSolr', 'bib_set'),
    ('EResourcesToSolr', 'eres_set'),
    ('ItemsToSolr', 'item_set'),
    ('ItemStatusesToSolr', 'istatus_set'),
    ('ItypesToSolr', 'itype_set'),
    ('LocationsToSolr', 'location_set'),
    ('ItemsBibsToSolr', 'item_set'),
    ('BibsAndAttachedToSolr', 'bib_set'),
    ('BibsAndAttachedToSolr', 'er_bib_set')
])
def test_basic_export_get_records(et_code, rset_code, basic_exporter_class,
                                  record_sets, new_exporter):
    """
    For Basic Exporter classes that get data from Sierra, the
    `get_records` method should return a record set containing the
    expected records.
    """
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    db_records = exporter.get_records()
    assert len(db_records) > 0
    assert all([rec in db_records for rec in record_sets[rset_code]])


@pytest.mark.exports
@pytest.mark.get_records
@pytest.mark.batch
def test_batch_export_get_records(batch_exporter_class, record_sets,
                                  new_exporter):
    """
    For Batch Exporter classes that get data from Sierra, the
    `get_records` method should return a dict of all applicable record
    sets.
    """
    expclass = batch_exporter_class('AllMetadataToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    rsets = exporter.get_records()

    expected_rsets = {
        'LocationsToSolr': record_sets['location_set'],
        'ItypesToSolr': record_sets['itype_set'],
        'ItemStatusesToSolr': record_sets['istatus_set'],
    }
    assert len(expected_rsets.keys()) == len(rsets.keys())
    for key, rset in rsets.items():
        assert len(rset) > 0
        assert all([r in rset for r in expected_rsets[key]])


@pytest.mark.deletions
@pytest.mark.get_records
@pytest.mark.basic
@pytest.mark.parametrize('et_code, rset_code', [
    ('BibsToSolr', 'bib_del_set'),
    ('EResourcesToSolr', 'eres_del_set'),
    ('ItemsToSolr', 'item_del_set'),
    ('ItemStatusesToSolr', None),
    ('ItypesToSolr', None),
    ('LocationsToSolr', None),
    ('ItemsBibsToSolr', 'item_del_set'),
    ('BibsAndAttachedToSolr', 'bib_del_set'),
])
def test_basic_export_get_deletions(et_code, rset_code, basic_exporter_class,
                                    record_sets, new_exporter):
    """
    For basic Exporter classes that get data from Sierra, the
    `get_deletions` method should return a record set containing the
    expected records.
    """
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    db_records = exporter.get_deletions()
    if rset_code is None:
        assert db_records is None
    else:
        assert all([rec in db_records for rec in record_sets[rset_code]])


@pytest.mark.deletions
@pytest.mark.get_records
@pytest.mark.batch
def test_batch_export_get_deletions(batch_exporter_class, record_sets,
                                    new_exporter):
    """
    For batch Exporter classes that get data from Sierra, the
    `get_deletions` method should return the expected record set.

    Note: I'm anticipating having more batch exporters to test soon,
    so this is set up to be ready for parametrization to work when
    something actually returns data.
    """
    expclass = batch_exporter_class('AllMetadataToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    rsets = exporter.get_deletions()
    expected_rsets = None
    if expected_rsets is None:
        assert rsets is None
    else:
        assert len(expected_rsets.keys()) == len(rsets.keys())
        for key, rset in rsets.items():
            if expected_rsets[key] is None:
                assert rset is None
            else:
                assert len(rset) > 0
                assert all([r in rset for r in expected_rsets[key]])


@pytest.mark.exports
@pytest.mark.do_export
@pytest.mark.basic
@pytest.mark.parametrize('et_code, rset_code, rectypes, do_reindex', [
    ('BibsToSolr', 'bib_set', ('bib', 'marc'), False),
    ('EResourcesToSolr', 'eres_set', ('eresource',), False),
    ('ItemsToSolr', 'item_set', ('item',), False),
    ('ItemStatusesToSolr', 'istatus_set', ('itemstatus',), True),
    ('ItypesToSolr', 'itype_set', ('itype',), True),
    ('LocationsToSolr', 'location_set', ('location',), True),
])
def test_basic_tosolr_export_records(et_code, rset_code, rectypes, do_reindex,
                                     basic_exporter_class, record_sets,
                                     new_exporter, solr_conns, solr_search,
                                     solr_assemble_specific_record_data,
                                     assert_records_are_indexed,
                                     assert_records_are_not_indexed):
    """
    For basic ToSolrExporter classes, the `export_records` method
    should load the expected records into the expected Solr index. This
    uses the `solr_assemble_specific_record_data` fixture to help
    preload some data into Solr. We want to make sure that exporters
    where indexes are supposed to be fully refreshed with each run
    delete the old data and only load the data in the recordset, while
    other exporters add records to the existing recordset.
    """
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    records = record_sets[rset_code]
    
    # Do some setup to put some meaningful data into the index first.
    # We want some records that overlap with the incoming record set
    # and some that don't.
    num_existing = records.count() / 2
    overlap_recs = records[0:num_existing]
    only_new_recs = records[num_existing:]
    old_rec_pks = [unicode(pk) for pk in range(99991,99995)]
    only_old_rec_data = [{'django_id': pk} for pk in old_rec_pks]
    data = only_old_rec_data + [{'django_id': r.pk} for r in overlap_recs]
    assembler = solr_assemble_specific_record_data(data, rectypes)

    # Check the setup to make sure existing records are indexed and new
    # records are not.
    for index in exporter.indexes.values():
        conn = solr_conns[getattr(index, 'using', 'default')]
        results = solr_search(conn, '*')
        only_old_recs = [r for r in results if r['django_id'] in old_rec_pks]
        assert len(only_old_recs) == len(old_rec_pks)
        assert_records_are_indexed(index, overlap_recs, results=results)
        assert_records_are_not_indexed(index, only_new_recs, results=results)

    exporter.export_records(records)
    exporter.commit_indexes()

    for i, index in enumerate(exporter.indexes.values()):
        conn = solr_conns[getattr(index, 'using', 'default')]
        results = solr_search(conn, '*')
        only_old_recs = [r for r in results if r['django_id'] in old_rec_pks]
        assert len(only_old_recs) == 0 if do_reindex else len(old_rec_pks)
        assert_records_are_indexed(index, overlap_recs, results=results)
        assert_records_are_indexed(index, only_new_recs, results=results)


@pytest.mark.exports
@pytest.mark.do_export
@pytest.mark.batch
def test_batch_tosolr_export_records(batch_exporter_class, record_sets,
                                     new_exporter,
                                     assert_all_exported_records_are_indexed):
    """
    For batch ToSolrExporter classes, the `export_records` method
    should load the expected records into the expected Solr index.
    This is just a simple check to make sure all child exporters
    processed the appropriate recordset; the children are tested more
    extensively elsewhere.
    """
    records = {
        'LocationsToSolr': record_sets['location_set'],
        'ItypesToSolr': record_sets['itype_set'],
        'ItemStatusesToSolr': record_sets['istatus_set'],
    }
    expclass = batch_exporter_class('AllMetadataToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    exporter.export_records(records)
    for key, child in exporter.children.items():
        child.commit_indexes()
        assert_all_exported_records_are_indexed(child, records[key])


@pytest.mark.deletions
@pytest.mark.do_export
@pytest.mark.basic
@pytest.mark.parametrize('et_code, rset_code, rectypes', [
    ('BibsToSolr', 'bib_del_set', ('bib', 'marc')),
    ('EResourcesToSolr', 'eres_del_set', ('eresource',)),
    ('ItemsToSolr', 'item_del_set', ('item',)),
])
def test_basic_tosolr_delete_records(et_code, rset_code, rectypes,
                                     basic_exporter_class, record_sets,
                                     new_exporter,
                                     solr_assemble_specific_record_data,
                                     assert_records_are_indexed,
                                     assert_deleted_records_are_not_indexed):
    """
    For basic ToSolrExporter classes that have loaded data into Solr,
    the `delete_records` method should delete records from the
    appropriate index or indexes.
    """
    records = record_sets[rset_code]
    data = ({'id': r.id, 'record_number': r.get_iii_recnum()} for r in records)
    assembler = solr_assemble_specific_record_data(data, rectypes)
    
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')

    for index in exporter.indexes.values():
        assert_records_are_indexed(index, records)

    exporter.delete_records(records)
    exporter.commit_indexes()
    assert_deleted_records_are_not_indexed(exporter, records)


@pytest.mark.deletions
@pytest.mark.do_export
@pytest.mark.batch
def test_batch_tosolr_delete_records(batch_exporter_class, record_sets,
                                     new_exporter,
                                     solr_assemble_specific_record_data,
                                     assert_records_are_indexed,
                                     assert_deleted_records_are_not_indexed):
    """
    For batch ToSolrExporter classes that have loaded data into Solr,
    the `delete_records` method should delete records from the
    appropriate index or indexes.

    This is a placeholder, for now. The only existing batch exporter
    (AllMetadataToSolr) doesn't do deletions.
    """
    pass


@pytest.mark.exceptions
def test_tosolr_index_update_errors(basic_exporter_class, record_sets,
                                    new_exporter, setattr_model_instance,
                                    assert_records_are_indexed,
                                    assert_records_are_not_indexed):
    """
    When updating indexes via a ToSolrExporter, if one record causes an
    error during preparation (e.g. via the haystack SearchIndex obj),
    the export process should: 1) skip that record, and 2) log the
    error as a warning on the exporter. Other records in the same batch
    should still be indexed.
    """
    records = record_sets['item_set']
    expclass = basic_exporter_class('ItemsToSolr')
    invalid_loc_code = '_____'
    exporter = new_exporter(expclass, 'full_export', 'waiting')

    def prepare_location_code(obj):
        code = obj.location_id
        if code == invalid_loc_code:
            raise Exception('Code not valid')
        return code

    exporter.indexes['Items'].prepare_location_code = prepare_location_code
    setattr_model_instance(records[0], 'location_id', invalid_loc_code)
    exporter.export_records(records)
    exporter.commit_indexes()

    assert_records_are_not_indexed(exporter.indexes['Items'], [records[0]])
    assert_records_are_indexed(exporter.indexes['Items'], records[1:])
    assert len(exporter.indexes['Items'].last_batch_errors) == 1


@pytest.mark.exports
@pytest.mark.do_export
@pytest.mark.parametrize('et_code, rset_code', [
    ('ItemsBibsToSolr', 'item_set'),
    ('BibsAndAttachedToSolr', 'bib_set'),
    ('BibsAndAttachedToSolr', 'er_bib_set')
])
def test_attached_solr_export_records(et_code, rset_code, basic_exporter_class,
                                      record_sets, new_exporter,
                                      assert_all_exported_records_are_indexed):
    """
    For AttachedRecordExporter classes that load data into Solr, the
    `export_records` method should load the expected records into the
    expected Solr indexes. This is just a simple check to make sure all
    child exporters processed the appropriate recordsets; the children
    are tested more extensively elsewhere.
    """
    records = record_sets[rset_code]
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    exporter.export_records(records)
    for child in exporter.children.values():
        child.commit_indexes()
    assert_all_exported_records_are_indexed(exporter, records)


@pytest.mark.deletions
@pytest.mark.do_export
@pytest.mark.parametrize('et_code, rset_code, rectypes', [
    ('ItemsBibsToSolr', 'item_del_set', ('item',)),
    ('BibsAndAttachedToSolr', 'bib_del_set', ('bib', 'marc')),
])
def test_attached_solr_delete_records(et_code, rset_code, rectypes,
                                      basic_exporter_class, record_sets,
                                      new_exporter,
                                      solr_assemble_specific_record_data,
                                      assert_records_are_indexed,
                                      assert_deleted_records_are_not_indexed):
    """
    For Exporter classes that have loaded data into Solr, the
    `delete_records` method should delete records from the appropriate
    index or indexes.
    """
    records = record_sets[rset_code]
    data = ({'id': r.id, 'record_number': r.get_iii_recnum()} for r in records)
    assembler = solr_assemble_specific_record_data(data, rectypes)
    
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')

    for index in exporter.main_child.indexes.values():
        assert_records_are_indexed(index, records)

    exporter.delete_records(records)
    for child in exporter.children.values():
        child.commit_indexes()
    assert_deleted_records_are_not_indexed(exporter, records)


@pytest.mark.parametrize('et_code', [
    'BibsToSolr',
    'EResourcesToSolr',
    'ItemsToSolr',
    'ItemStatusesToSolr',
    'ItypesToSolr',
    'LocationsToSolr'])
def test_max_chunk_settings_overrides(et_code, settings, basic_exporter_class,
                                      new_exporter):
    """
    Using EXPORTER_MAX_RC_CONFIG and EXPORTER_MAX_DC_CONFIG settings
    should override values set on the class when an exporter is
    instantiated.
    """
    expclass = basic_exporter_class(et_code)
    test_et_code = expclass.__name__
    new_rc_val, new_dc_val = 77777, 88888
    settings.EXPORTER_MAX_RC_CONFIG[test_et_code] = new_rc_val
    settings.EXPORTER_MAX_DC_CONFIG[test_et_code] = new_dc_val
    
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    assert exporter.max_rec_chunk == new_rc_val
    assert exporter.max_del_chunk == new_dc_val
    assert new_rc_val != expclass.max_rec_chunk
    assert new_dc_val != expclass.max_del_chunk


@pytest.mark.parametrize('et_code', [
    'BibsToSolr',
    'EResourcesToSolr',
    'ItemsToSolr',
    'ItemStatusesToSolr',
    'ItypesToSolr',
    'LocationsToSolr'])
def test_max_chunk_settings_defaults(et_code, settings, basic_exporter_class,
                                     new_exporter):
    """
    If NOT using EXPORTER_MAX_RC_CONFIG and EXPORTER_MAX_DC_CONFIG
    settings, the `max_rec_chunk` and `max_del_chunk` values for a
    given job should come from the exporter class.
    """
    settings.EXPORTER_MAX_RC_CONFIG = {}
    settings.EXPORTER_MAX_DC_CONFIG = {}
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    assert exporter.max_rec_chunk == expclass.max_rec_chunk
    assert exporter.max_del_chunk == expclass.max_del_chunk


@pytest.mark.return_vals
@pytest.mark.parametrize('vals_list, expected', [
    ([ { 'ids': ['b1', 'b2'] },
       { 'ids': ['b4', 'b5'] },
       { 'ids': ['b3'] }
    ], { 'ids': ['b1', 'b2', 'b4', 'b5', 'b3'] }),

    ([ { 'names': [{'first': 'Bob', 'last': 'Jones'}] },
       { 'names': [{'first': 'Sarah', 'last': 'Kim'}] },
       { 'names': [{'first': 'Sally', 'last': 'Smith'}] }
    ], { 'names': [{'first': 'Bob', 'last': 'Jones'},
                   {'first': 'Sarah', 'last': 'Kim'},
                   {'first': 'Sally', 'last': 'Smith'}] }),

    ([ { 'grades': {'Bob Jones': ['A', 'B'],
                    'Sarah Kim': ['B', 'A', 'C']} },
       { 'grades': {'Bob Jones': ['A']} },
       { 'grades': {'Sally Smith': ['A', 'A', 'B']} }
    ], { 'grades': {'Bob Jones': ['A', 'B', 'A'],
                    'Sarah Kim': ['B', 'A', 'C'],
                    'Sally Smith': ['A', 'A', 'B']} }),

    ([ { 'list1': ['a', 'b'], 'list2': [1, 2] },
       { 'list1': ['c', 'd'], 'list2': [3, 4] },
       { 'list1': ['e', 'f'], 'list2': [5, 6] },
    ], { 'list1': ['a', 'b', 'c', 'd', 'e', 'f'],
         'list2': [1, 2, 3, 4, 5, 6] }),

    ([ { 'list1': ['a', 'b'], 'list2': [1, 2] },
       { 'list1': ['c', 'd']},
       { 'list1': ['e', 'f'], 'list2': [5, 6] },
    ], { 'list1': ['a', 'b', 'c', 'd', 'e', 'f'],
         'list2': [1, 2, 5, 6] }),

    ([ { 'list1': [], 'list2': [1, 2] },
       { 'list1': ['c', 'd'], 'list2': [3, 4] },
       { 'list1': ['e', 'f'], 'list2': [5, 6] },
    ], { 'list1': ['c', 'd', 'e', 'f'],
         'list2': [1, 2, 3, 4, 5, 6] }),

    ([ { 'list1': ['a', 'b'] },
       { 'list2': [1, 2] },
    ], { 'list1': ['a', 'b'],
         'list2': [1, 2] }),

    ([ { 'list1': [1, 2, 3] },
       None,
       { 'list1': [4, 5, 6] }
    ], { 'list1': [1, 2, 3, 4, 5, 6] }),

    ([ None, None, None ], None),
], ids=[
    'one key, arrays of values',
    'one key, arrays of dicts',
    'one key, nested dicts',
    'two keys, normal',
    'two keys, one is absent',
    'two keys, one is a blank value',
    'two keys, mutually exclusive',
    'one vals_list is None',
    'all vals_lists are None',
])
def test_exporter_default_compile_vals(vals_list, expected, new_exporter,
                                       derive_exporter_class):
    """
    The default `Exporter.compile_vals` method should take a list of
    dicts and return a single dict that represents a merger of all
    dicts in the list.
    """
    expclass = derive_exporter_class('Exporter', 'export.exporter')
    exp = new_exporter(expclass, 'full_export', 'waiting')
    assert exp.compile_vals(vals_list) == expected


@pytest.mark.compound
@pytest.mark.return_vals
@pytest.mark.do_export
@pytest.mark.parametrize('classname, method, children_and_retvals, expected', [
    ('AttachedRecordExporter', 'export_records',
        [ ('C1', {'colors': ['red', 'green']}), ('C2', {'sounds': ['woosh']}),
          ('C3', None) ],
        { 'C1': {'colors': ['red', 'green']},
          'C2': {'sounds': ['woosh']},
          'C3': None }
    ),
    ('AttachedRecordExporter', 'delete_records',
        [ ('C1', {'colors': ['red', 'green']}), ('C2', {'sounds': ['woosh']}),
          ('C3', None) ],
        { 'C1': {'colors': ['red', 'green']} }
    ),
    ('BatchExporter', 'export_records',
        [ ('C1', {'colors': ['red', 'green']}), ('C2', {'sounds': ['woosh']}),
          ('C3', None) ],
        { 'C1': {'colors': ['red', 'green']},
          'C2': {'sounds': ['woosh']},
          'C3': None }
    ),
    ('BatchExporter', 'delete_records',
        [ ('C1', {'colors': ['red', 'green']}), ('C2', {'sounds': ['woosh']}),
          ('C3', None) ],
        { 'C1': {'colors': ['red', 'green']},
          'C2': {'sounds': ['woosh']},
          'C3': None }
    ),
], ids=[
    'AttachedRE export_records: all children run and return their vals',
    'AttachedRE delete_records: only main child runs and returns vals',
    'BatchE export_records: all children run and return their vals',
    'BatchE delete_records: all children run and return their vals',
])
def test_compound_ops_and_return_vals(classname, method, children_and_retvals,
                                      expected, derive_compound_exporter_class,
                                      derive_child_exporter_class, new_exporter,
                                      mocker):
    """
    The `export_records` and `delete_records` methods for
    AttachedRecordExporter and BatchExporter should return a dict where
    each key contains the return vals for each child that ran.
    """
    child_classes = []
    for name, retvals in children_and_retvals:
        child = derive_child_exporter_class(newname=name)
        mocker.patch.object(child, method)
        getattr(child, method).return_value = retvals
        child_classes.append(child)

    expclass = derive_compound_exporter_class(classname, 'export.exporter',
                                              children=child_classes)
    exp = new_exporter(expclass, 'full_export', 'waiting')
    return_vals = getattr(exp, method)([])
    assert return_vals == expected
    for name, child in exp.children.items():
        if name in expected.keys():
            getattr(child, method).assert_called_with([])
        else:
            getattr(child, method).assert_not_called()


@pytest.mark.compound
@pytest.mark.return_vals
@pytest.mark.parametrize('classname, children, vals_list, expected', [
    ('AttachedRecordExporter', ('C1', 'C2'),
        [ {'C1': {'colors': ['red', 'blue']}, 'C2': {'sounds': ['pop']}},
          {'C1': {'colors': ['tan']}, 'C2': {'sounds': ['squee', 'pop']}} ],
        { 'C1': {'colors': ['red', 'blue', 'tan']},
          'C2': {'sounds': ['pop', 'squee', 'pop']} }),

    ('AttachedRecordExporter', ('C1', 'C2'),
        [ {'C1': {'colors': ['red', 'blue']}, 'C2': {'colors': ['yellow']}},
          {'C1': {'colors': ['tan']}, 'C2': {'colors': ['pink', 'brown']}} ],
        { 'C1': {'colors': ['red', 'blue', 'tan']},
          'C2': {'colors': ['yellow', 'pink', 'brown']} }),

    ('AttachedRecordExporter', ('C1', 'C2'),
        [ {'C1': {'colors': ['red', 'blue']}, 'C2': {'colors': ['yellow']}},
          {'C1': {'sounds': ['pop', 'bang']}, 'C2': {'colors': ['red']}} ],
        { 'C1': {'colors': ['red', 'blue'], 'sounds': ['pop', 'bang']},
          'C2': {'colors': ['yellow', 'red']} }),

    ('AttachedRecordExporter', ('C1', 'C2'),
        [ {'C1': None, 'C2': {'colors': ['yellow']}},
          {'C1': None, 'C2': {'colors': ['pink', 'brown']}} ],
        { 'C1': None, 'C2': {'colors': ['yellow', 'pink', 'brown']} }),

    ('AttachedRecordExporter', ('C1', 'C2'),
        [ {'C1': {'colors': ['red']}, 'C2': {'colors': ['yellow']}},
          {'C1': None, 'C2': {'colors': ['pink', 'brown']}} ],
        { 'C1': {'colors': ['red']},
          'C2': {'colors': ['yellow', 'pink', 'brown']} }),

    ('AttachedRecordExporter', ('C1', 'C2'),
        [ {'C1': None, 'C2': None}, {'C1': None, 'C2': None} ],
        { 'C1': None, 'C2': None }),

    ('AttachedRecordExporter', ('C1', 'C2'),
        [ {'C1': {'colors': ['red']}},
          {'C1': {'colors': ['tan']}, 'C2': {'colors': ['pink', 'brown']}} ],
        { 'C1': {'colors': ['red', 'tan']},
          'C2': {'colors': ['pink', 'brown']} }),

    ('AttachedRecordExporter', ('C1', 'C2'),
        [ {'C1': {'colors': ['red']}}, {'C1': {'colors': ['tan']}} ],
        { 'C1': {'colors': ['red', 'tan']} }),

    ('BatchExporter', ('C1', 'C2'),
        [ {'C1': {'colors': ['red', 'blue']}},
          {'C1': {'colors': ['tan', 'red']}} ],
        { 'C1': {'colors': ['red', 'blue', 'tan', 'red']} }),

    ('BatchExporter', ('C1', 'C2'),
        [ {'C1': {'colors': ['red', 'blue']}},
          {'C1': {'sounds': ['pop']}} ],
        { 'C1': {'colors': ['red', 'blue'], 'sounds': ['pop']} }),

    ('BatchExporter', ('C1', 'C2'),
        [ {'C1': None}, {'C1': {'colors': ['red']}} ],
        { 'C1': {'colors': ['red']} }),

    ('BatchExporter', ('C1', 'C2'),
        [ {'C1': None}, {'C1': None} ],
        { 'C1': None }),

    ('BatchExporter', ('C1', 'C2'),
        [ {'C1': {'colors': ['red', 'blue']}}, {'C2': {'sounds': ['pop']}} ],
        { 'C1': {'colors': ['red', 'blue']}, 'C2': {'sounds': ['pop']} }),

    ('BatchExporter', ('C1', 'C2'),
        [ {'C1': None}, {'C2': {'sounds': ['pop']}} ],
        { 'C1': None, 'C2': {'sounds': ['pop']} }),

    ('BatchExporter', ('C1', 'C2'),
        [ {'C1': None}, {'C2': None} ],
        { 'C1': None, 'C2': None }),

    ('BatchExporter', ('C1', 'C2'),
        [ {'C1': {'colors': ['red', 'blue']}, 'C2': {'sounds': ['pop']}},
          {'C1': {'colors': ['tan']}, 'C2': {'sounds': ['squee', 'pop']}} ],
        { 'C1': {'colors': ['red', 'blue', 'tan']},
          'C2': {'sounds': ['pop', 'squee', 'pop']} }),

    ('BatchExporter', ('C1', 'C2'),
        [ {'C1': {'colors': ['red', 'blue']}, 'C2': {'colors': ['yellow']}},
          {'C1': {'colors': ['tan']}, 'C2': {'colors': ['pink', 'brown']}} ],
        { 'C1': {'colors': ['red', 'blue', 'tan']},
          'C2': {'colors': ['yellow', 'pink', 'brown']} }),

    ('BatchExporter', ('C1', 'C2'),
        [ {'C1': {'colors': ['red', 'blue']}, 'C2': {'colors': ['yellow']}},
          {'C1': {'sounds': ['pop', 'bang']}, 'C2': {'colors': ['red']}} ],
        { 'C1': {'colors': ['red', 'blue'], 'sounds': ['pop', 'bang']},
          'C2': {'colors': ['yellow', 'red']} }),

    ('BatchExporter', ('C1', 'C2'),
        [ {'C1': None, 'C2': {'colors': ['yellow']}},
          {'C1': None, 'C2': {'colors': ['pink', 'brown']}} ],
        { 'C1': None, 'C2': {'colors': ['yellow', 'pink', 'brown']} }),

    ('BatchExporter', ('C1', 'C2'),
        [ {'C1': {'colors': ['red']}, 'C2': {'colors': ['yellow']}},
          {'C1': None, 'C2': {'colors': ['pink', 'brown']}} ],
        { 'C1': {'colors': ['red']},
          'C2': {'colors': ['yellow', 'pink', 'brown']} }),

    ('BatchExporter', ('C1', 'C2'),
        [ {'C1': None, 'C2': None}, {'C1': None, 'C2': None} ],
        { 'C1': None, 'C2': None }),

], ids=[
    'AttachedRE: children have different keys',
    'AttachedRE: children have the same keys',
    'AttachedRE: same child has different keys',
    'AttachedRE: one child has return value None',
    'AttachedRE: one child returns None once',
    'AttachedRE: both children return None',
    'AttachedRE: one child missing an entry (did not run, once)',
    'AttachedRE: one child has no entries (did not run, at all)',
    'BatchE: only one child ran; same keys',
    'BatchE: only one child ran; different keys',
    'BatchE: only one child ran; one chunk has no ret vals',
    'BatchE: only one child ran; no ret vals',
    'BatchE: different children ran',
    'BatchE: different children ran, one has no ret vals',
    'BatchE: different children ran, neither has ret vals',
    'BatchE: both children ran; children have different keys',
    'BatchE: both children ran; children have the same keys',
    'BatchE: both children ran; same child has different keys',
    'BatchE: both children ran; one child has return value None',
    'BatchE: both children ran; one child returns None once',
    'BatchE: both children ran; both children return None',
])
def test_compound_compile_vals(classname, children, vals_list, expected,
                               derive_compound_exporter_class,
                               derive_child_exporter_class, new_exporter):
    """
    The `compile_vals` method for AttachedRecordExporter and
    BatchExporter should take a list of vals dicts and return a merged
    vals dict, assuming that each key/value pair represents the return
    values for each child exporter that ran during an export or delete
    operation. Keys are the names of each child.
    """
    child_classes = [derive_child_exporter_class(newname=n) for n in children]
    expclass = derive_compound_exporter_class(classname, 'export.exporter',
                                              children=child_classes)
    exp = new_exporter(expclass, 'full_export', 'waiting')
    assert exp.compile_vals(vals_list) == expected


@pytest.mark.compound
@pytest.mark.callback
@pytest.mark.parametrize('classname, children, vals', [
    ('AttachedRecordExporter', ('C1', 'C2'),
        { 'C1': {'colors': ['red']}, 'C2': {'sounds': 'pop'} }),
    ('AttachedRecordExporter', ('C1', 'C2'),
        { 'C1': {'colors': ['red']}, 'C2': None }),
    ('AttachedRecordExporter', ('C1', 'C2'),
        { 'C1': {'colors': ['red']} }),
    ('AttachedRecordExporter', ('C1', 'C2'), None),
    ('BatchExporter', ('C1', 'C2'),
        { 'C1': {'colors': ['red']}, 'C2': {'sounds': 'pop'} }),
    ('BatchExporter', ('C1', 'C2'),
        { 'C1': {'colors': ['red']}, 'C2': None }),
    ('BatchExporter', ('C1', 'C2'),
        { 'C1': {'colors': ['red']} }),
    ('BatchExporter', ('C1', 'C2'), None),
], ids=[
    'AttachedRE: both children have vals',
    'AttachedRE: one child has vals, the other has None',
    'AttachedRE: one child has vals, the other is missing',
    'AttachedRE: vals is None',
    'BatchE: both children have vals',
    'BatchE: one child has vals, the other has None',
    'BatchE: one child has vals, the other is missing',
    'BatchE: vals is None',
])
def test_compound_final_callback(classname, children, vals,
                                 derive_compound_exporter_class,
                                 derive_child_exporter_class, new_exporter,
                                 mocker):
    """
    The `final_callback` method for the given Compound exporter type
    (AttachedRecordExporter or BatchExporter) should run the
    `final_callback` method on each child, passing the appropriate
    portion of `vals` to each.
    """
    child_classes = []
    for name in children:
        child = derive_child_exporter_class(newname=name)
        mocker.patch.object(child, 'final_callback')
        child_classes.append(child)
    expclass = derive_compound_exporter_class(classname, 'export.exporter',
                                              children=child_classes)
    exp = new_exporter(expclass, 'full_export', 'waiting')
    exp.final_callback(vals=vals)
    for name, child in exp.children.items():
        expected_vals = None if vals is None else vals.get(name, None) 
        child.final_callback.assert_called_with(vals=expected_vals,
                                                status='success')


@pytest.mark.return_vals
def test_ertosolr_export_returns_h_lists(basic_exporter_class, record_sets,
                                         new_exporter):
    """
    The EResourcesToSolr exporter `export_records` method should return
    a dict with an `h_lists` key, which has a dict mapping eresources
    (record nums) to holdings/checkins (record nums).
    """
    records = record_sets['eres_set']
    expclass = basic_exporter_class('EResourcesToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    vals = exporter.export_records(records)
    assert 'h_lists' in vals
    for rec in records:
        er_recnum = rec.record_metadata.get_iii_recnum(True)
        assert er_recnum in vals['h_lists']
        exp_holdings = [h.record_metadata.get_iii_recnum(True)
                        for h in rec.holding_records.all()]
        assert vals['h_lists'][er_recnum] == exp_holdings


@pytest.mark.return_vals
def test_ertosolr_delete_returns_deletions(basic_exporter_class, record_sets,
                                           new_exporter):
    """
    The EResourcesToSolr exporter `delete_records` method should return
    a dict with a `deletions` key, which has the list of eresources
    (record nums) that were deleted in the batch.
    """
    records = record_sets['eres_del_set']
    expclass = basic_exporter_class('EResourcesToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')

    vals = exporter.delete_records(records)
    assert 'deletions' in vals
    exp_deletions = [r.get_iii_recnum(True) for r in records]
    assert vals['deletions'] == exp_deletions


@pytest.mark.callback
def test_ertosolr_export_callback_commits_to_redis(basic_exporter_class,
                                                   new_exporter, redis_obj):
    """
    The EResourcesToSolr exporter `final_callback` method should commit
    updated holdings lists for new and updated eresources to Redis, and
    it should add them to or otherwise update the reverse holdings list
    appropriately.
    """
    existing = {
        'eresource_holdings_list:e2': ['c1', 'c2', 'c3'],
        'eresource_holdings_list:e3': ['c4', 'c5'],
        'reverse_holdings_list:0': {
            'c1': 'e2', 'c2': 'e2', 'c3': 'e2', 'c4': 'e3', 'c5': 'e3'
        }
    }

    for key, val in existing.items():
        redis_obj(key).set(val)

    vals = { 'h_lists': { 'e1': ['c6', 'c4'], 'e3': ['c5', 'c7'] } }
    expected = {
        'eresource_holdings_list:e1': ['c6', 'c4'],
        'eresource_holdings_list:e2': ['c1', 'c2', 'c3'],
        'eresource_holdings_list:e3': ['c5', 'c7'],
        'reverse_holdings_list:0': {
            'c1': 'e2', 'c2': 'e2', 'c3': 'e2', 'c4': 'e1', 'c5': 'e3',
            'c6': 'e1', 'c7': 'e3'
        }
    }

    expclass = basic_exporter_class('EResourcesToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    exporter.final_callback(vals=vals, status='success')

    for key in redis_obj.conn.keys():
        assert key in expected
        assert redis_obj(key).get() == expected[key]


@pytest.mark.callback
def test_ertosolr_delete_callback_commits_to_redis(basic_exporter_class,
                                                   new_exporter, redis_obj):
    """
    The EResourcesToSolr exporter `final_callback` method should remove
    deleted eresources' holdings lists in Redis, and it should remove
    the applicable holdings records from the reverse holdings list.
    """
    existing = {
        'eresource_holdings_list:e1': ['c6', 'c4'],
        'eresource_holdings_list:e2': ['c1', 'c2', 'c3'],
        'eresource_holdings_list:e3': ['c5', 'c7'],
        'reverse_holdings_list:0': {
            'c1': 'e2', 'c2': 'e2', 'c3': 'e2', 'c4': 'e1', 'c5': 'e3',
            'c6': 'e1', 'c7': 'e3'
        }
    }

    for key, val in existing.items():
        redis_obj(key).set(val)

    vals = { 'deletions': ['e1', 'e3'] }
    expected = {
        'eresource_holdings_list:e2': ['c1', 'c2', 'c3'],
        'reverse_holdings_list:0': {
            'c1': 'e2', 'c2': 'e2', 'c3': 'e2'
        }
    }

    expclass = basic_exporter_class('EResourcesToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    exporter.final_callback(vals=vals, status='success')

    for key in redis_obj.conn.keys():
        assert key in expected
        assert redis_obj(key).get() == expected[key]
