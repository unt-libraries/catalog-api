"""
Tests the blacklight.parsers functions.
"""

import pytest
import pymarc

from blacklight import sierra2marc_alpha_solrmarc_02 as s2m


# FIXTURES AND TEST DATA
pytestmark = pytest.mark.django_db


@pytest.fixture
def get_args_for_pipeline(sierra_records_by_recnum_range):
    """
    Pytest fixture; returns a function that takes a Sierra record
    number (`recnum`) and provides the arguments to pass to a pipeline
    method, for testing: `r` (the DB bib record instance) and
    `marc_record` (the PyMARC version of the same record).
    """
    def _get_args_for_pipeline(recnum):
        r = sierra_records_by_recnum_range(recnum)[0]
        mrecord = s2m.S2MarcBatchBlacklightSolrMarc(r).compile_original_marc(r)
        return (r, mrecord)
    return _get_args_for_pipeline


@pytest.fixture
def blasm_pipeline_class():
    """
    Pytest fixture; returns the BlacklightASMPipeline class.
    """
    return s2m.BlacklightASMPipeline


@pytest.fixture
def plbundleconverter_class():
    """
    Pytest fixture; returns the PipelineBundleConverter class.
    """
    return s2m.PipelineBundleConverter


# TESTS

@pytest.mark.parametrize('kwargs', [
    {'data': 'abcdefg'},
    {'data': 'abcdefg', 'indicators': '12'},
    {'data': 'abcdefg', 'subfields': ['a', 'Test']},
    {'data': 'abcdefg', 'indicators': '12', 'subfields': ['a', 'Test']}
])
def test_make_pmfield_creates_control_field(kwargs):
    """
    When passed a `data` parameter, `make_pmfield` should create a
    pymarc control field, even if a `subfields` and/or `indicators`
    value is also passed.
    """
    field = s2m.make_pmfield('008', **kwargs)
    assert field.tag == '008'
    assert field.data == kwargs['data']
    assert not hasattr(field, 'indicators')
    assert not hasattr(field, 'subfields')


@pytest.mark.parametrize('kwargs', [
    {},
    {'indicators': '12'},
    {'subfields': ['a', 'Test1', 'b', 'Test2']}
])
def test_make_pmfield_creates_varfield(kwargs):
    """
    When NOT passed a `data` parameters, `make_pmfield` should create a
    pymarc variable-length field. If indicators are not provided,
    defaults should be blank ([' ', ' ']). If subfields are not
    provided, default should be an empty list.
    """
    field = s2m.make_pmfield('100', **kwargs)
    expected_ind = kwargs.get('indicators', '  ')
    expected_sf = kwargs.get('subfields', [])
    assert field.tag == '100'
    assert field.indicator1 == expected_ind[0]
    assert field.indicator2 == expected_ind[1]
    assert field.subfields == expected_sf


def test_blasmpipeline_do_creates_compiled_dict(blasm_pipeline_class):
    """
    The `do` method of BlacklightASMPipeline should return a dict
    compiled from the return value of each of the `get` methods--each
    key/value pair from each return value added to the finished value.
    """
    class DummyPipeline(blasm_pipeline_class):
        fields = ['dummy1', 'dummy2', 'dummy3']
        prefix = 'get_'

        def get_dummy1(self, r, marc_record):
            return {'d1': 'd1v'}

        def get_dummy2(self, r, marc_record):
            return { 'd2a': 'd2av', 'd2b': 'd2bv' }

        def get_dummy3(self, r, marc_record):
            return { 'stuff': ['thing'] }

    dummy_pipeline = DummyPipeline()
    bundle = dummy_pipeline.do('test', 'test')
    assert bundle == { 'd1': 'd1v', 'd2a': 'd2av', 'd2b': 'd2bv',
                       'stuff': ['thing'] }


def test_blasmpipeline_get_id(get_args_for_pipeline, blasm_pipeline_class):
    """
    BlacklightASMPipeline.get_id should return the bib Record ID
    formatted according to III's specs.
    """
    pipeline = blasm_pipeline_class()
    r, marc_record = get_args_for_pipeline('b4371446')
    val = pipeline.get_id(r, marc_record)
    assert val == {'id': '.b4371446'}


def test_blasmpipeline_get_suppressed(get_args_for_pipeline,
                                      blasm_pipeline_class):
    """
    BlacklightASMPipeline.get_suppressed should return 'false' if the
    record is not suppressed.
    """
    pipeline = blasm_pipeline_class()
    r, marc_record = get_args_for_pipeline('b4371446')
    val = pipeline.get_suppressed(r, marc_record)
    assert val == {'suppressed': 'false'}


@pytest.mark.parametrize('mapping, bundle, expected', [
    ( (('900', ('name', 'title')),),
      {'name': 'N1', 'title': 'T1'},
      [{'tag': '900', 'data': [('a', 'N1'), ('b', 'T1')]}] ),
    ( (('900', ('names', 'titles')),),
      {'names': ['N1', 'N2'], 'titles': ['T1', 'T2', 'T3']},
      [{'tag': '900', 'data': [('a', 'N1'), ('a', 'N2'),
                               ('b', 'T1'), ('b', 'T2'), ('b', 'T3')]}] ),
    ( (('900', ('names', 'titles')),
       ('900', ('subjects', 'eras')),),
      {'names': ['N1', 'N2'], 'titles': ['T1', 'T2'], 'subjects': ['S1'],
       'eras': ['E1', 'E2']},
      [{'tag': '900', 'data': [('a', 'N1'), ('a', 'N2'),
                               ('b', 'T1'), ('b', 'T2')]},
       {'tag': '900', 'data': [('c', 'S1'), ('d', 'E1'), ('d', 'E2')]}] ),
    ( (('900', ('names', 'titles')),
       ('950', ('subjects', 'eras')),),
      {'names': ['N1', 'N2'], 'titles': ['T1', 'T2'], 'subjects': ['S1'],
       'eras': ['E1', 'E2']},
      [{'tag': '900', 'data': [('a', 'N1'), ('a', 'N2'),
                               ('b', 'T1'), ('b', 'T2')]},
       {'tag': '950', 'data': [('a', 'S1'), ('b', 'E1'), ('b', 'E2')]}] ),
    ( (('900', ('names',)),),
      {'names': ['N1', 'N2']},
      [{'tag': '900', 'data': [('a', 'N1'),]},
       {'tag': '900', 'data': [('a', 'N2'),]}] ),
    ( (('900', ('names',)),
       ('900', ('titles',))),
      {'names': ['N1', 'N2'], 'titles': ['T1', 'T2', 'T3']},
      [{'tag': '900', 'data': [('a', 'N1'),]},
       {'tag': '900', 'data': [('a', 'N2'),]},
       {'tag': '900', 'data': [('b', 'T1'),]},
       {'tag': '900', 'data': [('b', 'T2'),]}] ),
    ( (('900', ('names',)),
       ('900', ('titles',)),
       ('900', ('subjects', 'eras')),),
      {'names': ['N1', 'N2'], 'titles': ['T1', 'T2'], 'subjects': ['S1'],
       'eras': ['E1', 'E2']},
      [{'tag': '900', 'data': [('a', 'N1'),]},
       {'tag': '900', 'data': [('a', 'N2'),]},
       {'tag': '900', 'data': [('b', 'T1'),]},
       {'tag': '900', 'data': [('b', 'T2'),]},
       {'tag': '900', 'data': [('c', 'S1'), ('d', 'E1'), ('d', 'E2')]}] ),
    ( (('900', ('names',)),
       ('900', ('titles',)),
       ('950', ('subjects',)),
       ('950', ('eras',)),),
      {'names': ['N1', 'N2'], 'titles': ['T1', 'T2'], 'subjects': ['S1'],
       'eras': ['E1', 'E2']},
      [{'tag': '900', 'data': [('a', 'N1'),]},
       {'tag': '900', 'data': [('a', 'N2'),]},
       {'tag': '900', 'data': [('b', 'T1'),]},
       {'tag': '900', 'data': [('b', 'T2'),]},
       {'tag': '950', 'data': [('a', 'S1'),]},
       {'tag': '950', 'data': [('b', 'E1'),]},
       {'tag': '950', 'data': [('b', 'E2'),]}] ),
    ( (('900', ('auth', 'contrib')),
       ('900', ('auth_display',)),
       ('950', ('subjects',)),
       ('950', ('eras', 'regions')),
       ('950', ('topics', 'genres')),),
      {'auth': ['A1'], 'contrib': ['C1', 'C2'],
       'auth_display': ['A1', 'C1', 'C2'], 'subjects': ['S1', 'S2', 'S3'],
       'eras': ['E1'], 'regions': ['R1', 'R2'], 'topics': ['T1'],
       'genres': ['G1']},
      [{'tag': '900', 'data': [('a', 'A1'), ('b', 'C1'), ('b', 'C2')]},
       {'tag': '900', 'data': [('c', 'A1')]},
       {'tag': '900', 'data': [('c', 'C1')]},
       {'tag': '900', 'data': [('c', 'C2')]},
       {'tag': '950', 'data': [('a', 'S1')]},
       {'tag': '950', 'data': [('a', 'S2')]},
       {'tag': '950', 'data': [('a', 'S3')]},
       {'tag': '950', 'data': [('b', 'E1'), ('c', 'R1'), ('c', 'R2')]},
       {'tag': '950', 'data': [('d', 'T1'), ('e', 'G1')]}] ),
    ( (('900', ('auth', 'contrib')),
       ('900', ('auth_display',)),
       ('950', ('subjects',)),
       ('950', ('eras', 'regions')),
       ('950', ('topics', 'genres')),),
      {'auth': ['A1'], 'contrib': ['C1', 'C2'],
       'auth_display': ['A1', 'C1', 'C2'], 'subjects': ['S1', 'S2', 'S3'],
       'regions': ['R1', 'R2'], 'topics': ['T1'], 'genres': ['G1']},
      [{'tag': '900', 'data': [('a', 'A1'), ('b', 'C1'), ('b', 'C2')]},
       {'tag': '900', 'data': [('c', 'A1')]},
       {'tag': '900', 'data': [('c', 'C1')]},
       {'tag': '900', 'data': [('c', 'C2')]},
       {'tag': '950', 'data': [('a', 'S1')]},
       {'tag': '950', 'data': [('a', 'S2')]},
       {'tag': '950', 'data': [('a', 'S3')]},
       {'tag': '950', 'data': [('c', 'R1'), ('c', 'R2')]},
       {'tag': '950', 'data': [('d', 'T1'), ('e', 'G1')]}] ),
    ( (('900', ('auth', 'contrib')),
       ('900', ('auth_display',)),
       ('950', ('subjects',)),
       ('950', ('eras', 'regions')),
       ('950', ('topics', 'genres')),),
      {'auth': ['A1'], 'contrib': ['C1', 'C2'],
       'auth_display': ['A1', 'C1', 'C2'], 'subjects': ['S1', 'S2', 'S3'],
       'topics': ['T1'], 'genres': ['G1']},
      [{'tag': '900', 'data': [('a', 'A1'), ('b', 'C1'), ('b', 'C2')]},
       {'tag': '900', 'data': [('c', 'A1')]},
       {'tag': '900', 'data': [('c', 'C1')]},
       {'tag': '900', 'data': [('c', 'C2')]},
       {'tag': '950', 'data': [('a', 'S1')]},
       {'tag': '950', 'data': [('a', 'S2')]},
       {'tag': '950', 'data': [('a', 'S3')]},
       {'tag': '950', 'data': [('d', 'T1'), ('e', 'G1')]}] ),
    ( (('900', ('auth', 'contrib')),
       ('900', ('auth_display',)),
       ('950', ('subjects',)),
       ('950', ('eras', 'regions')),
       ('950', ('topics', 'genres')),),
      {'subjects': ['S1', 'S2', 'S3'], 'topics': ['T1'], 'genres': ['G1']},
      [{'tag': '950', 'data': [('a', 'S1')]},
       {'tag': '950', 'data': [('a', 'S2')]},
       {'tag': '950', 'data': [('a', 'S3')]},
       {'tag': '950', 'data': [('d', 'T1'), ('e', 'G1')]}] ),
], ids=[
    '1 field with >1 subfields (single vals)',
    '1 field with >1 subfields (multiple vals => repeated subfields)',
    '>1 of same field with >1 subfields (single vals and multiple vals)',
    '>1 of diff fields with >1 subfields (single vals and multiple vals)',
    '1 field with 1 subfield (multiple vals => repeated field)',
    '>1 of same field with 1 subfield (multiple vals => repeated fields)',
    '>1 of same field with mixed subfields',
    '>1 of diff fields with 1 subfield (multiple vals => repeated field)',
    'mixed fields and subfields',
    'missing subfield is skipped',
    'missing row is skipped',
    'entire missing field is skipped'
])
def test_plbundleconverter_do_maps_correctly(mapping, bundle, expected,
                                             plbundleconverter_class):
    """
    PipelineBundleConverter.do should convert the given data dict to
    a list of pymarc Field objects correctly based on the provided
    mapping.
    """
    converter = plbundleconverter_class(mapping=mapping)
    fields = converter.do(bundle)
    for field, exp in zip(fields, expected):
        assert field.tag == exp['tag']
        assert list(field) == exp['data']



