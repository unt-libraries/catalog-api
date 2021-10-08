"""
Contains tests for utils.test_helpers.solr_factories.
"""

from __future__ import absolute_import

import datetime
import itertools

import pytest
import pytz
from six import text_type
from six.moves import range
from utils.test_helpers import solr_factories as f


# FIXTURES AND TEST DATA


@pytest.fixture
def schema():
    """
    Pytest fixture that returns a set of solr field definitions as
    though from the Solr schema API. Irrelevant elements `stored`,
    `indexed`, and `required` are not included.
    """
    return {
        'uniqueKey': 'haystack_id',
        'fields': [
            {'name': 'haystack_id',
             'type': 'string',
             'multiValued': False},
            {'name': 'django_id',
             'type': 'string',
             'multiValued': False},
            {'name': 'django_ct',
             'type': 'string',
             'multiValued': False},
            {'name': 'code',
             'type': 'string',
             'multiValued': False},
            {'name': 'label',
             'type': 'string',
             'multiValued': False},
            {'name': 'type',
             'type': 'string',
             'multiValued': False},
            {'name': 'id',
             'type': 'long',
             'multiValued': False},
            {'name': 'creation_date',
             'type': 'date',
             'multiValued': False},
            {'name': 'title',
             'type': 'text_en',
             'multiValued': False},
            {'name': 'notes',
             'type': 'text_en',
             'multiValued': True},
            {'name': 'status_code',
             'type': 'string',
             'multiValued': False},
            {'name': 'children_ids',
             'type': 'long',
             'multiValued': True},
            {'name': 'children_codes',
             'type': 'string',
             'multiValued': True},
            {'name': 'parent_id',
             'type': 'long',
             'multiValued': False},
            {'name': 'parent_title',
             'type': 'text_en',
             'multiValued': False},
            {'name': 'suppressed',
             'type': 'boolean',
             'multiValued': False}],
        'dynamicFields': [
            {'name': '*_unstem_search',
             'type': 'textNoStem',
             'multiValued': True},
            {'name': '*_display',
             'type': 'string',
             'multiValued': True},
            {'name': '*_search',
             'type': 'string',
             'multiValued': True},
            {'name': '*_facet',
             'type': 'string',
             'multiValued': True}
        ]}


@pytest.fixture
def data_emitter():
    """
    Pytest fixture function that generates and returns an appropriate
    DataEmitter object.
    """
    def _data_emitter(alphabet=None, emitter_defaults=None):
        return f.DataEmitter(alphabet, emitter_defaults)
    return _data_emitter


@pytest.fixture
def gen_factory(data_emitter):
    """
    Pytest fixture function that generates and returns an appropriate
    SolrDataGenFactory object.
    """
    def _gen_factory(emitter=None):
        emitter = emitter or data_emitter()
        return f.SolrDataGenFactory(emitter)
    return _gen_factory


@pytest.fixture
def profile(schema, gen_factory):
    """
    Pytest fixture function that generates and returns an appropriate
    `SolrProfile` object.
    """
    def _profile(name='test', user_fields=None, unique_fields=None, gens=None,
                 default_field_gens=None, my_schema=None, solr_types=None):
        gens = gens or gen_factory()
        my_schema = my_schema or schema
        return f.SolrProfile(name, schema=my_schema, user_fields=user_fields,
                             unique_fields=unique_fields, gen_factory=gens,
                             solr_types=solr_types,
                             default_field_gens=default_field_gens)
    return _profile


@pytest.fixture
def schema_types_error():
    """
    Pytest fixture that returns the SolrProfile.SchemaTypesError class.
    """
    return f.SolrProfile.SchemaTypesError


@pytest.fixture
def solr_types():
    """
    Pytest fixture that returns the default solr field type mapping,
    used in defining SolrProfile objects.
    """
    default = f.SolrProfile.DEFAULT_SOLR_FIELD_TYPE_MAPPING
    return {t: params.copy() for t, params in default.items()}


@pytest.fixture
def fixture_factory(profile):
    """
    Pytest fixture function that generates and returns an appropriate
    SolrFixtureFactory object.
    """
    solr_profile = profile

    def _fixture_factory(profile=None):
        profile = profile or solr_profile()
        return f.SolrFixtureFactory(profile)
    return _fixture_factory


# TESTS

@pytest.mark.parametrize('emtype, defaults, overrides, vcheck', [
    ('int', {'mx': 5}, None, lambda v: v <= 5),
    ('int', None, {'mx': 5}, lambda v: v <= 5),
    ('int', {'mx': 5}, {'mn': 3}, lambda v: v <= 5 and v >= 3),
    ('int', {'mx': 10}, {'mn': 3, 'mx': 5}, lambda v: v <= 5 and v >= 3),
    ('int', {'mn': 5, 'mx': 10}, None, lambda v: v >= 5 and v <= 10),
    ('string', {'mn': 5}, None, lambda v: len(v) >= 5),
    ('string', {'alphabet': 'abcd'}, None,
     lambda v: all([char in 'abcd' for char in v])),
    ('string', {'alphabet': 'abcd'}, {'alphabet': 'ab'},
     lambda v: all([char in 'ab' for char in v]))
])
def test_dataemitter_parameters(emtype, defaults, overrides, vcheck,
                                data_emitter):
    """
    When instantiating a `DataEmitter` object, setting emitter defaults
    individually via the `emitter_defaults` param should override
    those particular defaults. Then, emitting values via the `emit`
    method should utilize those defaults, UNLESS they are overridden in
    the method call.
    """
    em_defaults = defaults if defaults is None else {emtype: defaults}
    em = data_emitter(emitter_defaults=em_defaults)
    params = overrides or {}
    values = [em.emit(emtype, **params) for _ in range(0, 100)]
    assert all([vcheck(v) for v in values])


@pytest.mark.parametrize('choices, repeatable, try_num, exp_num', [
    (list(range(0, 10)), True, 1000, 1000),
    (list(range(0, 10)), False, 1000, 10),
    (list(range(0, 10000)), True, 1000, 1000),
    (list(range(0, 10000)), False, 1000, 1000),
    (list(range(0, 1000)), True, 1000, 1000),
    (list(range(0, 1000)), False, 1000, 1000),
])
def test_solrdatagenfactory_choice(choices, repeatable, try_num, exp_num,
                                   gen_factory):
    """
    The `SolrDataGenFactory` `choice` method should return a gen
    function that chooses from the given choices list. If `repeatable`
    is True, then choices can be repeated.
    """
    gen = gen_factory().choice(choices, repeatable)
    values = [v for v in [gen({}) for _ in range(0, try_num)] if v is not None]
    assert len(values) == exp_num
    assert all([v in choices for v in values])
    if not repeatable:
        assert len(set(values)) == exp_num


@pytest.mark.parametrize('choices, multi_num, repeatable, try_num, exp_num, '
                         'exp_last_num', [
                             (list(range(0, 10)), 5, True, 1000, 1000, 5),
                             (list(range(0, 10)), 5, False, 1000, 2, 5),
                             (list(range(0, 10)), 6, False, 1000, 2, 4),
                             (list(range(0, 10000)), 5, True, 1000, 1000, 5),
                             (list(range(0, 10000)), 5, False, 1000, 1000, 5),
                             (list(range(0, 1000)), 5, True, 1000, 1000, 5),
                             (list(range(0, 1000)), 5, False, 1000, 200, 5),
                         ])
def test_solrdatagenfactory_multichoice(choices, multi_num, repeatable,
                                        try_num, exp_num, exp_last_num,
                                        gen_factory):
    """
    The `SolrDataGenFactory` `multi_choice` method should return a gen
    function that picks lists of choices, where each choice comes from
    the given choices list, and the number of items in each list is
    determined by a provided counter function. If `repeatable` is True,
    then choices can be repeated; if False, they cannot be repeated. If
    `repeatable` is False and the available choices run out, it should
    return an empty list each time it is called.
    """
    gen = gen_factory().multi_choice(choices, lambda: multi_num, repeatable)
    value_lists = [vl for vl in [gen({}) for _ in range(0, try_num)] if vl]
    values = [v for vlist in value_lists for v in vlist]
    last = value_lists.pop()
    assert len(value_lists) == exp_num - 1
    assert all([len(vl) == multi_num for vl in value_lists])
    assert len(last) == exp_last_num
    assert all([v in choices for v in values])
    if not repeatable:
        assert len(set(values)) == ((exp_num - 1) * multi_num) + exp_last_num


def test_solrdatagenfactory_type_string(gen_factory):
    """
    The `SolrDataGenFactory` `type` method with an `emtype` 'string'
    should return a gen function that returns an appropriate string
    value based on the mn/mx params.
    """
    alph = list('abcdefghijklmnopqrstuvwxyz')
    gen = gen_factory().type('string', mn=0, mx=10, alphabet=alph)
    values = [gen({}) for _ in range(0, 1000)]
    chars = [char for string in values for char in string]
    assert all([len(v) >= 0 and len(v) <= 10 for v in values])
    assert all([char in alph for char in chars])


def test_solrdatagenfactory_type_text(gen_factory):
    """
    The `SolrDataGenFactory` `type` method with an `emtype` 'text'
    should return a gen function that returns an appropriate string
    value based on the given params.
    """
    gen = gen_factory().type('text', mn_words=1, mx_words=2, mn_word_len=3,
                             mx_word_len=5)
    values = [gen({}) for _ in range(0, 1000)]
    words_lists = [v.split(' ') for v in values]
    words = [w for words_list in words_lists for w in words_list]
    assert all([len(w) >= 1 and len(w) <= 2 for w in words_lists])
    assert all([len(w) >= 3 and len(w) <= 5 for w in words])


def test_solrdatagenfactory_type_int(gen_factory):
    """
    The `SolrDataGenFactory` `type` method with an `emtype` 'int'
    should return a gen function that returns an appropriate integer
    value based on the mn/mx params.
    """
    gen = gen_factory().type('int', mn=0, mx=10)
    values = [gen({}) for _ in range(0, 1000)]
    assert all([v >= 0 and v <= 10 for v in values])


def test_solrdatagenfactory_type_boolean(gen_factory):
    """
    The `SolrDataGenFactory` `type` method with an `emtype` 'boolean'
    should return a gen function that returns an appropriate bool
    value.
    """
    gen = gen_factory().type('boolean')
    values = [gen({}) for _ in range(0, 10)]
    assert all([v in (True, False) for v in values])


def test_solrdatagenfactory_type_date(gen_factory):
    """
    The `SolrDataGenFactory` `type` method with an `emtype` 'date'
    should return a gen function that returns an appropriate datetime
    value based on the mn/mx params.
    """
    min_tuple = (2018, 10, 29, 00, 00)
    max_tuple = (2018, 10, 31, 00, 00)
    min_date = datetime.datetime(*min_tuple, tzinfo=pytz.utc)
    max_date = datetime.datetime(*max_tuple, tzinfo=pytz.utc)
    gen = gen_factory().type('date', mn=min_tuple, mx=max_tuple)
    values = [gen({}) for _ in range(0, 1000)]
    assert all([v >= min_date and v <= max_date for v in values])


def test_solrdatagenfactory_multitype(gen_factory):
    """
    The `SolrDataGenFactory` `multi_type` method should return a gen
    function that returns a list with the correct number of generated
    values, suitable for passing to a multi-valued Solr field.
    """
    num = 3
    gen = gen_factory().multi_type('int', lambda: num, mn=0, mx=10)
    value_lists = [[v for v in gen({})] for _ in range(0, 10)]
    values = [v for value_list in value_lists for v in value_list]
    assert all([len(vlist) == num for vlist in value_lists])
    assert all([v >= 0 and v <= 10 for v in values])


def test_solrdatagenfactory_static(gen_factory):
    """
    The `SolrDataGenFactory` `static` method should return a gen
    function that returns the correct static value when called.
    """
    gen = gen_factory().static('Hello world.')
    values = [gen({}) for _ in range(0, 10)]
    assert all([v == 'Hello world.' for v in values])


def test_solrdatagenfactory_staticcounter(gen_factory):
    """
    The `SolrDataGenFactory` `static_counter` method should create a
    counter function that always returns the provided number.
    """
    count = gen_factory().static_counter(5)
    counts = [count() for _ in range(0, 100)]
    assert counts == [5] * 100


def test_solrdatagenfactory_randomcounter(gen_factory):
    """
    The `SolrDataGenFactory` `random_counter` method should create a
    counter function that returns a random number between the min and
    max values provided.
    """
    count = gen_factory().random_counter(0, 10)
    counts = [count() for _ in range(0, 100)]
    assert [c <= 10 and c >= 0 for c in counts]


@pytest.mark.parametrize('num_cycles, max_total, mn, mx', [
    (5, 26, 1, 10),
    (5, 5, 1, 10),
    (5, 6, 1, 10),
    (100, 200, 1, 3),
])
def test_solrdatagenfactory_precisedistributioncounter(num_cycles, max_total,
                                                       mn, mx, gen_factory):
    """
    The `SolrDataGenFactory` `precise_distribution_counter` method
    should create a counter function that returns a more-or-less even
    (but not uniform) distribution of values, which should always sum
    to exactly the given `max_total` when run `num_cycles` times.
    Each count that's generated should fall within the given `mn` and
    `mx` values.
    """
    count = gen_factory().precise_distribution_counter(num_cycles, max_total,
                                                       mn, mx)
    counts = [count() for _ in range(0, 100)]
    first, second = counts[0:num_cycles], counts[num_cycles:]
    assert sum(first) == max_total
    assert sum(second) == 0
    assert [c <= mx and c >= mn for c in first]


@pytest.mark.parametrize('fname, val, expected', [
    ('code', None, None),
    ('code', 'abc', u'abc'),
    ('code', 123, u'123'),
    ('id', None, None),
    ('id', '123', 123),
    ('id', 123, 123),
    ('notes', None, None),
    ('notes', [None], None),
    ('notes', [None, None], None),
    ('notes', 'one', [u'one']),
    ('notes', ['one', 123], [u'one', u'123']),
    ('creation_date', datetime.datetime(2015, 1, 1, 0, 0),
     datetime.datetime(2015, 1, 1, 0, 0))
])
def test_solrprofile_field_topython(fname, val, expected, profile):
    """
    The `SolrProfile.Field` `to_python` method should return a value
    of the appropriate type based on the parameters passed to it.
    """
    assert profile().fields[fname].to_python(val) == expected


def test_solrprofile_init_fields_structure(profile):
    """
    Initializing a `SolrProfile` object should interpret values
    correctly from the provided schema fields and return the correct
    structure.
    """
    prof = profile('test', None, None)
    assert prof.fields['haystack_id']['name'] == 'haystack_id'
    assert prof.fields['haystack_id']['is_key'] == True
    assert prof.fields['haystack_id']['type'] == 'string'
    assert prof.fields['haystack_id']['pytype'] == text_type
    assert prof.fields['haystack_id']['emtype'] == 'string'
    assert prof.fields['haystack_id']['multi'] == False
    assert prof.fields['haystack_id']['unique'] == True
    assert prof.fields['id']['is_key'] == False
    assert prof.fields['notes']['type'] == 'text_en'
    assert prof.fields['notes']['multi'] == True


def test_solrprofile_init_fields_include_all(schema, profile):
    """
    Initializing a `SolrProfile` object should result in a field
    structure that includes all static schema fields when the
    `user_fields` parameter is None.
    """
    assert len(profile().fields) == len(schema['fields'])


def test_solrprofile_init_fields_include_selective(profile):
    """
    Initializing a `SolrProfile` object should result in a field
    structure that includes only the provided list of user fields.
    """
    user_fields = ['haystack_id', 'creation_date', 'code', 'label']
    prof = profile('test', user_fields, None)
    assert len(prof.fields) == len(user_fields)
    assert all([fname in prof.fields for fname in user_fields])


def test_solrprofile_init_fields_include_dynamic(profile):
    """
    Initializing a `SolrProfile` object should result in a field
    structure that includes fields matching defined dynamic fields.
    """
    user_fields = ['haystack_id', 'code', 'test_facet', 'test_display']
    prof = profile('test', user_fields, None)
    assert len(prof.fields) == len(user_fields)
    assert all([fname in prof.fields for fname in user_fields])


def test_solrprofile_init_fields_unique(profile):
    """
    Initializing a `SolrProfile` object should result in a field
    structure where the `unique` key is set to True for all fields
    in the provided `unique_fields` parameter.
    """
    unique_fields = ['haystack_id', 'code']
    prof = profile('test', None, unique_fields)
    assert all([prof.fields[fn]['unique'] == True for fn in unique_fields])
    assert all([prof.fields[fn]['unique'] == False for fn in prof.fields
                if fn not in unique_fields])


def test_solrprofile_init_fields_multi_unique_error(profile):
    """
    Attempting to instantiate a `SolrProfile` object and defining a
    field that is both `multi` and `unique` should result in an error.
    """
    with pytest.raises(NotImplementedError):
        prof = profile('test', ['notes'], ['notes'])


def test_solrprofile_init_fields_invalid_types_error(schema, profile,
                                                     solr_types,
                                                     schema_types_error):
    """
    Attempting to instantiate a `SolrProfile` object using a schema
    with field types not included in the provided (or default) Solr
    type mapping should raise an error if any of those fields are used
    in the profile.
    """
    invalid_name, invalid_type = 'invalid', 'invalid'
    schema['fields'].append({'name': invalid_name, 'type': invalid_type,
                             'multiValued': False})
    assert invalid_type not in solr_types
    with pytest.raises(schema_types_error):
        prof = profile(my_schema=schema)


def test_solrprofile_init_unused_fields_invalid_types_okay(schema, profile,
                                                           solr_types):
    """
    Instantiating a `SolrProfile` object using a schema with field
    types not included in the provided (or default) Solr type mapping
    should NOT raise an error if none of the offending fields are
    included as part of the profile.
    """
    invalid_name, invalid_type = 'invalid', 'invalid'
    fnames = [f['name'] for f in schema['fields']]
    schema['fields'].append({'name': invalid_name, 'type': invalid_type,
                             'multiValued': False})
    prof = profile(user_fields=fnames, my_schema=schema)
    assert invalid_type not in solr_types
    assert invalid_name not in prof.fields


def test_solrprofile_init_fields_with_custom_type(schema, profile, solr_types):
    """
    Instantiating a `SolrProfile` object using a schema with field
    types included in the provided (but non-default) Solr type mapping
    should NOT raise an error.
    """
    custom_name, custom_type = 'custom', 'custom'
    schema['fields'].append({'name': custom_name, 'type': custom_type,
                             'multiValued': False})
    assert custom_type not in solr_types
    solr_types[custom_type] = {'pytype': text_type, 'emtype': 'string'}
    prof = profile(my_schema=schema, solr_types=solr_types)
    assert custom_name in prof.fields
    assert prof.fields[custom_name]['pytype'] == text_type
    assert prof.fields[custom_name]['emtype'] == 'string'


def test_solrfixturefactory_make_basic_fields(data_emitter, gen_factory,
                                              profile, fixture_factory):
    """
    The `SolrFixtureFactory` `make` function should make and return a
    set of records with fields corresponding to a particular
    SolrProfile object. For this test we have no unique or multi-valued
    fields.
    """
    user_fields = ['id', 'code', 'title', 'creation_date', 'suppressed']
    alphabet = list('abcdefghijklmnopqrstuvwxyz')
    defaults = {
        'string': {'mn': 1, 'mx': 8},
        'int': {'mn': 1, 'mx': 999999},
        'date': {'mn': (2018, 10, 29, 00, 00), 'mx': (2018, 10, 31, 00, 00)},
        'text': {'mn_words': 1, 'mx_words': 5, 'mn_word_len': 2,
                 'mx_word_len': 6}
    }

    gens = gen_factory(data_emitter(alphabet, defaults))
    prof = profile('test', user_fields, None, gens)
    factory = fixture_factory(prof)
    records = factory.make(1000)

    values = {fname: [r[fname] for r in records] for fname in user_fields}
    title_words_lists = [v.split(' ') for v in values['title']]
    title_words = [w for words_list in title_words_lists for w in words_list]
    min_date = datetime.datetime(*defaults['date']['mn'], tzinfo=pytz.utc)
    max_date = datetime.datetime(*defaults['date']['mx'], tzinfo=pytz.utc)

    assert len(records) == 1000
    assert all([v >= 1 and v <= 999999 for v in values['id']])
    assert all([l in alphabet for v in values['code'] for l in v])
    assert all([len(v) >= 1 and len(v) <= 8 for v in values['code']])
    assert all([len(wl) >= 1 and len(wl) <= 5 for wl in title_words_lists])
    assert all([len(w) >= 2 and len(w) <= 6 for w in title_words])
    assert all([v >= min_date and v <= max_date
               for v in values['creation_date']])
    assert all([v in (True, False) for v in values['suppressed']])


def test_solrfixturefactory_make_multi_fields(profile, fixture_factory):
    """
    The `SolrFixtureFactory` `make` function should make and return a
    set of records with fields corresponding to a particular
    SolrProfile object. For this test we use multi-valued fields.
    """
    user_fields = ('notes', 'children_ids')
    prof = profile('test', user_fields)
    factory = fixture_factory(prof)
    records = factory.make(1000)

    values = {fname: [r[fname] for r in records] for fname in user_fields}
    ftypes = {fname: f['pytype'] for fname, f in prof.fields.items()}

    assert len(records) == 1000
    assert all([isinstance(v, ftypes['notes'])
                for vlist in values['notes'] for v in vlist])
    assert all([isinstance(v, ftypes['children_ids'])
               for vlist in values['children_ids'] for v in vlist])
    assert all([len(vlist) >= 1 and len(vlist) <= 10
               for vlist in values['notes']])
    assert all([len(vlist) >= 1 and len(vlist) <= 10
               for vlist in values['children_ids']])


@pytest.mark.parametrize('fields, defaults, attempted, expected', [
    (['id'], {'int': {'mn': 1, 'mx': 2000}}, 1000, 1000),
    (['code'], {'string': {'mn': 1, 'mx': 5}}, 1000, 1000),
    (['code'], {'string': {'mn': 1, 'mx': 1}}, 1000, 26),
    (['suppressed'], None, 1000, 2),
    (['creation_date'], None, 1000, 1000),
    (['title'], None, 1000, 1000),
])
def test_solrfixturefactory_make_unique_fields(fields, defaults, attempted,
                                               expected, data_emitter,
                                               gen_factory, profile,
                                               fixture_factory):
    """
    The `SolrFixtureFactory` `make` method should make and return a set
    of records with fields corresponding to a particular SolrProfile
    object. This tests unique fields.
    """
    default_alphabet = list('abcdefghijklmnopqrstuvwxyz')
    gens = gen_factory(data_emitter(default_alphabet, defaults))
    prof = profile('test', fields, fields, gens)
    factory = fixture_factory(prof)
    records = factory.make(attempted)

    values = {fname: [r[fname] for r in records] for fname in prof.fields}

    assert len(records) == expected
    # converting the generated values list to a set tests uniqueness
    assert all([len(set(values[fname])) == expected for fname in values])


@pytest.mark.parametrize('fields, unique, defaults, attempted, expected', [
    (['id', 'title'], ['id'], {'int': {'mn': 1, 'mx': 2000}}, 1000, 1000),
    (['id', 'title'], ['title'], None, 1000, 1000),
    (['code', 'title'], ['code'], {'string': {'mn': 1, 'mx': 1}}, 1000, 26),
    (['code', 'title'], ['code', 'title'], {'string': {'mn': 1, 'mx': 1}},
     1000, 26),
])
def test_solrfixturefactory_makemore(fields, unique, defaults, attempted,
                                     expected, data_emitter, gen_factory,
                                     profile, fixture_factory):
    """
    The `SolrFixtureFactory` `make_more` method should behave like the
    `make` method, except it takes a list of existing records, makes
    the requested number of additional records using a combination of
    the two lists for determining uniqueness, and then it returns the
    list of new records without modifying the original.
    """
    default_alphabet = list('abcdefghijklmnopqrstuvwxyz')
    gens = gen_factory(data_emitter(default_alphabet, defaults))
    prof = profile('test', fields, unique, gens)
    factory = fixture_factory(prof)

    attempted_first = int(expected / 2)
    attempted_second = attempted - attempted_first
    first_records = factory.make(attempted_first)
    second_records = factory.make_more(first_records, attempted_second)
    records = first_records + second_records

    values = {fname: [r[fname] for r in records] for fname in prof.fields}

    assert len(first_records) == attempted_first
    assert len(records) == expected
    assert all([len(values[fname]) == expected for fname in values])
    assert all([len(set(values[fname])) == expected for fname in unique])


def test_solrfixturefactory_custom_gens(gen_factory, profile, fixture_factory):
    """
    Fairly complex integration test to test the common features/cases
    for generating Solr fixtures.
    """
    gens = gen_factory()

    def haystack_id(record):
        return '{}.{}'.format(record['django_ct'], record['django_id'])

    def id_(record):
        return record['django_id']

    fields = ('haystack_id', 'django_ct', 'django_id', 'id', 'type', 'code',
              'creation_date', 'suppressed')
    unique = ('haystack_id', 'django_id', 'id', 'code')
    prof = profile('test', fields, unique)
    prof.set_field_gens(
        ('django_ct', gens.static('base.location')),
        ('django_id', gens.type('int', mn=1, mx=9999999999)),
        ('haystack_id', gens(haystack_id)),
        ('id', gens(id_)),
        ('code', gens.type('string', mn=3, mx=5)),
        ('type', gens.static('Location'))
    )
    factory = fixture_factory(prof)
    records = factory.make(1000)

    values = {fname: [r[fname] for r in records] for fname in fields}
    ftypes = {fname: f['pytype'] for fname, f in prof.fields.items()}

    assert len(records) == 1000
    assert all([len(values[fname]) == 1000 for fname in values])
    assert all([len(set(values[fname])) == 1000 for fname in unique])
    assert all([isinstance(v, ftypes[f]) for f in values for v in values[f]])

    assert all([v == 'base.location' for v in values['django_ct']])
    assert all([int(v) >= 1 and int(v) <= 9999999999
               for v in values['django_id']])
    assert all([r['haystack_id'] == '.'.join([r['django_ct'], r['django_id']])
               for r in records])
    assert all([r['id'] == int(r['django_id']) for r in records])
    assert all([len(v) >= 3 and len(v) <= 5 for v in values['code']])
    assert all([v == 'Location' for v in values['type']])


@pytest.mark.parametrize('profgen_fields, callgen_fields', [
    (None, None),
    (('django_ct', 'django_id'), None),
    (None, ('django_ct', 'django_id')),
    (('django_ct', 'django_id'), ('id', 'type')),
    (('django_ct', 'django_id'), ('django_ct', 'type')),
    (('django_ct', 'django_id'), ('django_ct', 'type', 'code', 'label')),
], ids=[
    'no field gen overrides',
    'profile gens only',
    'call gens only',
    'profile gens and call gens, no overlap',
    'profile gens and call gens, with overlap',
    'all field gens overridden'
])
def test_solrfixturefactory_fieldgen_precedence(profgen_fields, callgen_fields,
                                                data_emitter, gen_factory,
                                                profile, fixture_factory):
    """
    This tests to make sure gens and gen overrides fire using the
    correct precedence. Setting field gens on the `SolrProfile` object
    overrides the fixture factory's default auto generators. Then,
    passing field gens to the fixture

    Overrides
    for those can then be passed when calling the fixture factory's
    `make` methods.
    """
    fields = ('django_ct', 'django_id', 'id', 'type', 'code', 'label')
    profgen_fields = profgen_fields or tuple()
    callgen_fields = callgen_fields or tuple()

    expected_use_callgen = callgen_fields
    expected_use_profgen = tuple(set(profgen_fields) - set(callgen_fields))
    expected_use_basegen = tuple(set(fields) - set(profgen_fields)
                                 - set(callgen_fields))

    # Test logic: a profile-level gen returns 1; a call-level gen
    # returns 2. (These numbers will automatically be converted to the
    # correct type based on the field). We set up the default emitter
    # so that `int` fields generate a minimum of 3 and `string` fields
    # use a simple a-z alphabet to avoid possible conflicts. Then we
    # can check these values in the final record set to confirm which
    # level of gen was used.
    alphabet = 'abcdefghijklmnopqrstuvwxyz'
    emitter = data_emitter(alphabet, {'int': {'mn': 3}})
    gens = gen_factory(emitter)

    profgen = gens.static(1)
    callgen = gens.static(2)
    profgens = [(fname, profgen) for fname in profgen_fields]
    callgens = {fname: callgen for fname in callgen_fields}
    prof = profile('test', fields, None, gens, profgens)
    factory = fixture_factory(prof)
    records = factory.make(10, **callgens)

    values = {fname: [r[fname] for r in records] for fname in prof.fields}

    assert len(records) == 10
    assert all([int(v) == 1 for fname in expected_use_profgen
               for v in values[fname]])
    assert all([int(v) == 2 for fname in expected_use_callgen
               for v in values[fname]])
    assert all([v not in (1, 2) for fname in expected_use_basegen
               for v in values[fname]])


@pytest.mark.parametrize('profgen_fields, auto_fields, callgen_fields', [
    (('type', 'id', 'label'), None, None),
    (('type', 'id', 'label'), None, ('code', 'django_id')),
    (('type', 'id', 'label'), None, ('id', 'code')),
    (('code', 'type', 'label'), None, ('id', 'code')),
    (('type', 'id', 'label'), None, ('id', 'type')),
    (('code', 'type', 'label', 'id'), ('code', 'id'), ('id', 'code')),
], ids=[
    'profile gens only',
    'profile gens and call gens, no overlap',
    'profile gens and call gens, some overlap',
    'profile gens and call gens, different overlap',
    'profile gens and call gens, full overlap',
    'profile gens with auto and call gens',
])
def test_solrfixturefactory_fieldgen_order(profgen_fields, auto_fields,
                                           callgen_fields, data_emitter,
                                           gen_factory, profile,
                                           fixture_factory):
    """
    This tests to make sure field gen overrides fire in the correct
    order. Setting field gens on the `SolrProfile` object sets what
    order those field gens get called in when the fixture factory makes
    fixtures. Call-level overrides then fire in the order set via the
    profile. For fields that need to be generated in a particular order
    where it doesn't make sense to specify a custom gen at the profile
    level, you can include the field in the profile definition but use
    the keyword 'auto' in place of an actual gen.
    """
    fields = ('django_ct', 'django_id', 'id', 'type', 'code', 'label')
    profgen_fields = profgen_fields or tuple()
    callgen_fields = callgen_fields or tuple()
    auto_fields = auto_fields or tuple()

    # Test logic: A custom gen uses a global iterator to increase and
    # return a count number each time a field using that gen is called.
    # We set up the default emitter so that `int` fields generate a min
    # of 10000 and `string` fields use a simple a-z alphabet to avoid
    # possible conflicts. We use the custom counter gen for certain
    # fields at the profile level and at the call level, and then we
    # compare the numerical order of output values to the expected
    # field sort order for each record.
    alphabet = 'abcdefghijklmnopqrstuvwxyz'
    emitter = data_emitter(alphabet, {'int': {'mn': 10000}})
    gens = gen_factory(emitter)

    count = itertools.count()
    countgen = gens(lambda r: next(count))
    profgens = [(fname, 'auto' if fname in auto_fields else countgen)
                for fname in profgen_fields]
    callgens = {fname: countgen for fname in callgen_fields}
    prof = profile('test', fields, None, gens, profgens)
    factory = fixture_factory(prof)
    records = factory.make(100, **callgens)

    assert len(records) == 100
    assert all([[int(r[fn]) for fn in profgen_fields]
                == sorted([int(r[fn]) for fn in profgen_fields])
                for r in records])
