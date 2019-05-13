"""
Tests classes derived from `export.exporter.Exporter`.
"""

import pytest
from base.models import BibRecord

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


# TESTS

@pytest.mark.parametrize('et_code', [
    ('BibsToSolr'),
    ('EResourcesToSolr'),
    ('ItemsToSolr'),
    ('ItemStatusesToSolr'),
    ('ItypesToSolr'),
    ('LocationsToSolr'),
    ('ItemsBibsToSolr'),
    ('BibsAndAttachedToSolr')
])
def test_exporter_class_versions(et_code, new_exporter, basic_exporter_class):
    """
    For all exporter types / classes that are under test in this test
    module, what we get from the `basic_exporter_class` fixture should
    be derived from the `export` app.
    """
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    assert exporter.app_name == 'export'
    for child_etcode, child in getattr(exporter, 'children', {}).items():
        assert child.app_name == 'export'


@pytest.mark.exports
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
def test_export_get_records(et_code, rset_code, basic_exporter_class,
                            record_sets, new_exporter):
    """
    For Exporter classes that get data from Sierra, the `get_records`
    method should return a record set containing the expected records.
    """
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    db_records = exporter.get_records()
    print exporter.prefetch_related
    assert len(db_records) > 0
    assert all([rec in db_records for rec in record_sets[rset_code]])


@pytest.mark.deletions
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
def test_export_get_deletions(et_code, rset_code, basic_exporter_class,
                              record_sets, new_exporter):
    """
    For Exporter classes that get data from Sierra, the `get_deletions`
    method should return a record set containing the expected records.
    """
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    db_records = exporter.get_deletions()
    if rset_code is None:
        assert db_records == None
    else:
        assert all([rec in db_records for rec in record_sets[rset_code]])


@pytest.mark.exports
@pytest.mark.parametrize('et_code, rset_code', [
    ('BibsToSolr', 'bib_set'),
    ('EResourcesToSolr', 'eres_set'),
    ('ItemsToSolr', 'item_set'),
    ('ItemStatusesToSolr', 'istatus_set'),
    ('ItypesToSolr', 'itype_set'),
    ('LocationsToSolr', 'location_set'),
])
def test_tosolr_export_records(et_code, rset_code, basic_exporter_class,
                               record_sets, new_exporter,
                               assert_all_exported_records_are_indexed):
    """
    For ToSolrExporter classes, the `export_records` method should load
    the expected records into the expected Solr index.
    """
    records = record_sets[rset_code]
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    exporter.export_records(records)
    exporter.commit_indexes()
    assert_all_exported_records_are_indexed(exporter, records)


@pytest.mark.deletions
@pytest.mark.parametrize('et_code, rset_code, rectypes', [
    ('BibsToSolr', 'bib_del_set', ('bib', 'marc')),
    ('EResourcesToSolr', 'eres_del_set', ('eresource',)),
    ('ItemsToSolr', 'item_del_set', ('item',)),
])
def test_tosolr_delete_records(et_code, rset_code, rectypes,
                               basic_exporter_class, record_sets, new_exporter,
                               solr_assemble_specific_record_data,
                               assert_records_are_indexed,
                               assert_deleted_records_are_not_indexed):
    """
    For ToSolrExporter classes that have loaded data into Solr, the
    `delete_records` method should delete records from the appropriate
    index or indexes.
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


def test_tosolr_index_update_errors(basic_exporter_class, record_sets,
                                    new_exporter,setattr_model_instance,
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
    expected Solr indexes.
    """
    records = record_sets[rset_code]
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    exporter.export_records(records)
    for child in exporter.children.values():
        child.commit_indexes()
    assert_all_exported_records_are_indexed(exporter, records)


@pytest.mark.deletions
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
    expclass = derive_exporter_class('Exporter', 'export.exporter',
                                     attrs={'model': BibRecord})
    exp = new_exporter(expclass, 'full_export', 'waiting')
    assert exp.compile_vals(vals_list) == expected


@pytest.mark.return_vals
def test_attachedrecordexporter_export_return_vals(derive_exporter_class,
                                                   new_exporter):
    """
    The `AttachedRecordExporter.export_records` method should return a
    dictionary where each key/value pair is the name and return value
    for each child exporter.
    """
    expclass_child1 = derive_exporter_class(
        'Exporter',
        'export.exporter',
        newname='Child1',
        attrs={
            'model': BibRecord,
            'export_records': lambda s, r: {'colors': ['red', 'green']}
        }
    ),

    expclass_child2 = derive_exporter_class(
        'Exporter',
        'export.exporter',
        newname='Child2',
        attrs={
            'model': BibRecord,
            'export_records': lambda s, r: {'sounds': ['woosh']}
        }
    ),

    expclass_child3 = derive_exporter_class(
        'Exporter',
        'export.exporter',
        newname='Child3',
        attrs={
            'model': BibRecord,
            'export_records': lambda s, r: None
        }
    ),
    expclass = derive_exporter_class(
        'AttachedRecordExporter',
        'export.exporter',
        newname='Parent',
        attrs={'model': BibRecord}
    )
    expclass.children_config = (expclass.Child('Child1'),
                                expclass.Child('Child2'),
                                expclass.Child('Child3'))

    exp = new_exporter(expclass, 'full_export', 'waiting')
    return_vals = exp.export_records([])
    assert return_vals == {
        'Child1': {'colors': ['red', 'green']},
        'Child2': {'sounds': ['woosh']},
        'Child3': None
    }


@pytest.mark.return_vals
def test_attachedrecordexporter_delete_return_vals(derive_exporter_class,
                                                   new_exporter):
    """
    The `AttachedRecordExporter.delete_records` method should return a
    dictionary where each child is represented, but only the main
    child (first child) has values. The other children should be None.
    (For the `AttachedRecordExporter` class, deletions are only ever
    run for the main child.)
    """
    expclass_child1 = derive_exporter_class(
        'Exporter',
        'export.exporter',
        newname='Child1',
        attrs={
            'model': BibRecord,
            'delete_records': lambda s, r: {'colors': ['red', 'green']}
        }
    ),

    expclass_child2 = derive_exporter_class(
        'Exporter',
        'export.exporter',
        newname='Child2',
        attrs={
            'model': BibRecord,
            'delete_records': lambda s, r: {'sounds': ['woosh']}
        }
    ),

    expclass_child3 = derive_exporter_class(
        'Exporter',
        'export.exporter',
        newname='Child3',
        attrs={
            'model': BibRecord,
            'delete_records': lambda s, r: None
        }
    ),
    expclass = derive_exporter_class(
        'AttachedRecordExporter',
        'export.exporter',
        newname='Parent',
        attrs={'model': BibRecord}
    )
    expclass.children_config = (expclass.Child('Child1'),
                                expclass.Child('Child2'),
                                expclass.Child('Child3'))

    exp = new_exporter(expclass, 'full_export', 'waiting')
    return_vals = exp.delete_records([])
    assert return_vals == {
        'Child1': {'colors': ['red', 'green']},
        'Child2': None,
        'Child3': None
    }


@pytest.mark.return_vals
@pytest.mark.parametrize('c1_vlist, c2_vlist, expected', [
    ([ {'colors': ['red', 'blue']},
       {'colors': ['orange']} ],
     [ {'sounds': ['pop', 'boom']},
       {'sounds': ['squee', 'pop']} ],
     { 'Child1': {'colors': ['red', 'blue', 'orange']},
       'Child2': {'sounds': ['pop', 'boom', 'squee', 'pop']} }),

    ([ {'colors': ['red', 'blue']},
       {'colors': ['orange']} ],
     [ {'colors': ['yellow']},
       {'colors': ['pink', 'brown']} ],
     { 'Child1': {'colors': ['red', 'blue', 'orange']},
       'Child2': {'colors': ['yellow', 'pink', 'brown']} }),

    ([ None, None ],
     [ {'colors': ['yellow']},
       {'colors': ['pink', 'brown']} ],
     { 'Child1': None,
       'Child2': {'colors': ['yellow', 'pink', 'brown']} }),

    ([ None, None ],
     [ None, None ],
     { 'Child1': None,
       'Child2': None }),
], ids=[
    'children have different keys',
    'children have the same keys',
    'one child has no return values',
    'neither child has return values'
])
def test_attachedrecordexporter_compile_vals(c1_vlist, c2_vlist, expected,
                                             derive_exporter_class,
                                             new_exporter):
    """
    The `AttachedRecordExporter.compile_vals` method should take a list
    of dicts and return a merged dict, assuming that each key/value
    pair represents the return values for each child exporter. Keys are
    the names of each child.
    """
    expclass_child1 = derive_exporter_class(
        'Exporter',
        'export.exporter',
        newname='Child1',
        attrs={'model': BibRecord}
    ),
    expclass_child2 = derive_exporter_class(
        'Exporter',
        'export.exporter',
        newname='Child2',
        attrs={'model': BibRecord}
    ),
    expclass = derive_exporter_class(
        'AttachedRecordExporter',
        'export.exporter',
        newname='Parent',
        attrs={'model': BibRecord}
    )
    expclass.children_config = (expclass.Child('Child1'),
                                expclass.Child('Child2'))
    exp = new_exporter(expclass, 'full_export', 'waiting')
    vals_list = [{'Child1': c1_vlist[i], 'Child2': c2_vlist[i]} 
                    for i, _ in enumerate(c1_vlist)]
    assert exp.compile_vals(vals_list) == expected


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

