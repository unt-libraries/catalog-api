"""
Tests API features applicable to the `blacklight` app, alpha-solrmarc.
"""

import pytest
import ujson
import jsonpatch
from datetime import datetime

from shelflist.exporters import ItemsToSolr
from shelflist.search_indexes import ShelflistItemIndex
from shelflist.serializers import ShelflistItemSerializer

# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in ...
# django/sierra/blacklight/tests/conftest.py:

# URL/RESOURCE constants
API_ROOT = '/api/v1/'
RESOURCE_TYPES = {
    'search': { 'url': 'asm-search-suggestions',
                'resource': 'asmSearchSuggestions' },
    'browse': { 'url': 'asm-browse-suggestions',
                'resource': 'asmBrowseSuggestions' },
}


# PARAMETERS__* constants contain parametrization data for certain
# tests. Each should be a tuple, where the first tuple member is a
# header string that describes the parametrization values (such as
# what you'd pass as the first arg to pytest.mark.parametrize); the
# others are single-entry dictionaries where the key is the parameter-
# list ID (such as what you'd pass to pytest.mark.parametrize via its
# `ids` kwarg) and the value is the list of parameters for that ID.

# PARAMETERS__FILTER_TESTS: Parameters for testing API filter
# behavior that works as intended. The provided `search` query string
# matches the `test_data` record(s) they're supposed to match.
# NOTE: Because the shelflistitems resource is used to support the
# inventory app, the tests (particularly filter tests) are aimed at
# testing features as used by that app; they don't test every edge case
# and every possibility.
PARAMETERS__FILTER_TESTS = (
    'rtype, test_data, search, expected',

    # Filter by heading_type
    { 'search filter by heading_type: exact match': ('search', (
        ('TEST1', {'heading': 'TEST1', 'heading_type': 'title'}),
        ('TEST2', {'heading': 'TEST2', 'heading_type': 'title'}),
        ('TEST3', {'heading': 'TEST3', 'heading_type': 'author'}),
     ), 'heading_type=title', ['TEST1', 'TEST2']),
    }, { 'browse filter by heading_type: exact match': ('browse', (
        ('TEST1', {'heading': 'TEST1', 'heading_type': 'title'}),
        ('TEST2', {'heading': 'TEST2', 'heading_type': 'title'}),
        ('TEST3', {'heading': 'TEST3', 'heading_type': 'author'}),
     ), 'heading_type=title', ['TEST1', 'TEST2']),
    },

    # Filter by (content) facet values
    { 'search filter by (content) facet: one value, w/space': ('search', (
        ('TEST1', {'heading': 'TEST1', 'heading_type': 'title',
                   'public_subject_facet': ['subj a', 'subj b'] }),
        ('TEST2', {'heading': 'TEST2', 'heading_type': 'title',
                   'public_subject_facet': ['subj b', 'subj c'] }),
        ('TEST3', {'heading': 'TEST3', 'heading_type': 'author',
                   'public_subject_facet': ['subj c', 'subj d'] }),
     ), 'public_subject_facet="subj c"', ['TEST2', 'TEST3']),
    }, { 'browse filter by (content) facet: one value, w/space': ('browse', (
        ('TEST1', {'heading': 'TEST1', 'heading_type': 'title',
                   'public_subject_facet': ['subj a', 'subj b'] }),
        ('TEST2', {'heading': 'TEST2', 'heading_type': 'title',
                   'public_subject_facet': ['subj b', 'subj c'] }),
        ('TEST3', {'heading': 'TEST3', 'heading_type': 'author',
                   'public_subject_facet': ['subj c', 'subj d'] }),
     ), 'public_subject_facet="subj c"', ['TEST2', 'TEST3']),
    }, { 'search filter by (content) facet: multi values': ('search', (
        ('TEST1', {'heading': 'TEST1', 'heading_type': 'title',
                   'public_subject_facet': ['subj a', 'subj b'] }),
        ('TEST2', {'heading': 'TEST2', 'heading_type': 'title',
                   'public_subject_facet': ['subj b', 'subj c'] }),
        ('TEST3', {'heading': 'TEST3', 'heading_type': 'author',
                   'public_subject_facet': ['subj c', 'subj d'] }),
     ), 'public_subject_facet="subj a"&public_subject_facet="subj b"',
        ['TEST1']),
    }, { 'browse filter by (content) facet: multi values': ('browse', (
        ('TEST1', {'heading': 'TEST1', 'heading_type': 'title',
                   'public_subject_facet': ['subj a', 'subj b'] }),
        ('TEST2', {'heading': 'TEST2', 'heading_type': 'title',
                   'public_subject_facet': ['subj b', 'subj c'] }),
        ('TEST3', {'heading': 'TEST3', 'heading_type': 'author',
                   'public_subject_facet': ['subj c', 'subj d'] }),
     ), 'public_subject_facet="subj a"&public_subject_facet="subj b"',
        ['TEST1']),
    }, { 'search filter by (content) facet: multi facets': ('search', (
        ('TEST1', {'heading': 'TEST1', 'heading_type': 'title',
                   'public_subject_facet': ['subj a', 'subj b'],
                   'public_title_facet': ['title1', 'title2'] }),
        ('TEST2', {'heading': 'TEST2', 'heading_type': 'title',
                   'public_subject_facet': ['subj b', 'subj c'],
                   'public_title_facet': ['title2', 'title3'] }),
        ('TEST3', {'heading': 'TEST3', 'heading_type': 'author',
                   'public_subject_facet': ['subj c', 'subj d'],
                   'public_title_facet': ['title3', 'title4'] }),
     ), 'public_subject_facet="subj b"&public_title_facet=title3',
        ['TEST2']),
    }, { 'browse filter by (content) facet: multi facets': ('browse', (
        ('TEST1', {'heading': 'TEST1', 'heading_type': 'title',
                   'public_subject_facet': ['subj a', 'subj b'],
                   'public_title_facet': ['title1', 'title2'] }),
        ('TEST2', {'heading': 'TEST2', 'heading_type': 'title',
                   'public_subject_facet': ['subj b', 'subj c'],
                   'public_title_facet': ['title2', 'title3'] }),
        ('TEST3', {'heading': 'TEST3', 'heading_type': 'author',
                   'public_subject_facet': ['subj c', 'subj d'],
                   'public_title_facet': ['title3', 'title4'] }),
     ), 'public_subject_facet="subj b"&public_title_facet=title3',
        ['TEST2']),
    }, 

    # Filter by (content) facet values using fq
    { 'search filter by fq: one fq value': ('search', (
        ('TEST1', {'heading': 'TEST1', 'heading_type': 'title',
                   'material_type': ['a', 'b'] }),
        ('TEST2', {'heading': 'TEST2', 'heading_type': 'title',
                   'material_type': ['b', 'c'] }),
        ('TEST3', {'heading': 'TEST3', 'heading_type': 'author',
                   'material_type': ['c', 'd']}),
     ), 'fq=material_type:a OR material_type:b', ['TEST1', 'TEST2']),
    }, { 'browse filter by fq: one fq value': ('browse', (
        ('TEST1', {'heading': 'TEST1', 'heading_type': 'title',
                   'material_type': ['a', 'b'] }),
        ('TEST2', {'heading': 'TEST2', 'heading_type': 'title',
                   'material_type': ['b', 'c'] }),
        ('TEST3', {'heading': 'TEST3', 'heading_type': 'author',
                   'material_type': ['c', 'd'] }),
     ), 'fq=material_type:a OR material_type:b', ['TEST1', 'TEST2']),
    }, { 'search filter by fq: multiple fq values': ('search', (
        ('TEST1', {'heading': 'TEST1', 'heading_type': 'title',
                   'material_type': ['a', 'b'],
                   'bib_location_codes': ['w1', 'w2'] }),
        ('TEST2', {'heading': 'TEST2', 'heading_type': 'title',
                   'material_type': ['b', 'c'],
                   'bib_location_codes': ['w2', 'w3'] }),
        ('TEST3', {'heading': 'TEST3', 'heading_type': 'author',
                   'material_type': ['c', 'd'],
                   'bib_location_codes': ['w3', 'w4'] }),
     ), 'fq=material_type:b&fq=bib_location_codes:w1', ['TEST1']),
    }, { 'browse filter by fq: multiple fq values': ('browse', (
        ('TEST1', {'heading': 'TEST1', 'heading_type': 'title',
                   'material_type': ['a', 'b'],
                   'bib_location_codes': ['w1', 'w2'] }),
        ('TEST2', {'heading': 'TEST2', 'heading_type': 'title',
                   'material_type': ['b', 'c'],
                   'bib_location_codes': ['w2', 'w3'] }),
        ('TEST3', {'heading': 'TEST3', 'heading_type': 'author',
                   'material_type': ['c', 'd'],
                   'bib_location_codes': ['w3', 'w4'] }),
     ), 'fq=material_type:b&fq=bib_location_codes:w1', ['TEST1']),
    },

    # Filter by (content) facet values using fq AND facet field
    { 'search filter by fq AND facet field': ('search', (
        ('TEST1', {'heading': 'TEST1', 'heading_type': 'title',
                   'material_type': ['a', 'b'],
                   'bib_location_codes': ['w1', 'w2'] }),
        ('TEST2', {'heading': 'TEST2', 'heading_type': 'title',
                   'material_type': ['b', 'c'],
                   'bib_location_codes': ['w2', 'w3'] }),
        ('TEST3', {'heading': 'TEST3', 'heading_type': 'author',
                   'material_type': ['c', 'd'],
                   'bib_location_codes': ['w3', 'w4'] }),
     ), 'fq=material_type:b&bib_location_codes=w1', ['TEST1']),
    }, { 'browse filter by fq AND facet field': ('browse', (
        ('TEST1', {'heading': 'TEST1', 'heading_type': 'title',
                   'material_type': ['a', 'b'],
                   'bib_location_codes': ['w1', 'w2'] }),
        ('TEST2', {'heading': 'TEST2', 'heading_type': 'title',
                   'material_type': ['b', 'c'],
                   'bib_location_codes': ['w2', 'w3'] }),
        ('TEST3', {'heading': 'TEST3', 'heading_type': 'author',
                   'material_type': ['c', 'd'],
                   'bib_location_codes': ['w3', 'w4'] }),
     ), 'fq=material_type:b&bib_location_codes=w1', ['TEST1']),
    },

    # SEARCH QUERY BEHAVIOR
    { 'search: multiple matches on partial word': ('search', (
        ('TEST1', {'heading': 'Online Audio', 'heading_type': 'genre' }),
        ('TEST2', {'heading': 'Online Video', 'heading_type': 'genre' }),
        ('TEST3', {'heading': 'Online audio thing', 'heading_type': 'genre' }),
        ('TEST4', {'heading': 'Only something', 'heading_type': 'genre' }),
        ('TEST5', {'heading': 'Something else', 'heading_type': 'genre' }),
     ), 'search=onl', ['TEST1', 'TEST2', 'TEST3', 'TEST4']),
    }, { 'search: multiple matches on full word': ('search', (
        ('TEST1', {'heading': 'Online Audio', 'heading_type': 'genre' }),
        ('TEST2', {'heading': 'Online Video', 'heading_type': 'genre' }),
        ('TEST3', {'heading': 'Online audio thing', 'heading_type': 'genre' }),
        ('TEST4', {'heading': 'Only something', 'heading_type': 'genre' }),
        ('TEST5', {'heading': 'Something else', 'heading_type': 'genre' }),
     ), 'search=online ', ['TEST1', 'TEST2', 'TEST3']),
    }, { 'search: multiple matches on partial phrase': ('search', (
        ('TEST1', {'heading': 'Online Audio', 'heading_type': 'genre' }),
        ('TEST2', {'heading': 'Online Video', 'heading_type': 'genre' }),
        ('TEST3', {'heading': 'Online audio thing', 'heading_type': 'genre' }),
        ('TEST4', {'heading': 'Only something', 'heading_type': 'genre' }),
        ('TEST5', {'heading': 'Something else', 'heading_type': 'genre' }),
     ), 'search=online aud', ['TEST1', 'TEST3']),
    }, { 'search: multiple matches on partial phrase w/one quote': ('search', (
        ('TEST1', {'heading': 'Online Audio', 'heading_type': 'genre' }),
        ('TEST2', {'heading': 'Online Video', 'heading_type': 'genre' }),
        ('TEST3', {'heading': 'Online audio thing', 'heading_type': 'genre' }),
        ('TEST4', {'heading': 'Only something', 'heading_type': 'genre' }),
        ('TEST5', {'heading': 'Something else', 'heading_type': 'genre' }),
     ), 'search="online aud', ['TEST1', 'TEST3']),
    }, { 'search: no matches on partial phrase in quotes': ('search', (
        ('TEST1', {'heading': 'Online Audio', 'heading_type': 'genre' }),
        ('TEST2', {'heading': 'Online Video', 'heading_type': 'genre' }),
        ('TEST3', {'heading': 'Online audio thing', 'heading_type': 'genre' }),
        ('TEST4', {'heading': 'Only something', 'heading_type': 'genre' }),
        ('TEST5', {'heading': 'Something else', 'heading_type': 'genre' }),
     ), 'search="online aud"', []),
    }, { 'search: multiple matches on full phrase in quotes': ('search', (
        ('TEST1', {'heading': 'Online Audio', 'heading_type': 'genre' }),
        ('TEST2', {'heading': 'Online Video', 'heading_type': 'genre' }),
        ('TEST3', {'heading': 'Online audio thing', 'heading_type': 'genre' }),
        ('TEST4', {'heading': 'Only something', 'heading_type': 'genre' }),
        ('TEST5', {'heading': 'Something else', 'heading_type': 'genre' }),
     ), 'search="online audio"', ['TEST1', 'TEST3']),
    }, { 'search: matches on one word, not left-anchored': ('search', (
        ('TEST1', {'heading': 'Online Audio', 'heading_type': 'genre' }),
        ('TEST2', {'heading': 'Online Video', 'heading_type': 'genre' }),
        ('TEST3', {'heading': 'Online audio thing', 'heading_type': 'genre' }),
        ('TEST4', {'heading': 'Only something', 'heading_type': 'genre' }),
        ('TEST5', {'heading': 'Something else', 'heading_type': 'genre' }),
     ), 'search=something', ['TEST4', 'TEST5']),
    }, { 'search: match on complex phrase w/quotes': ('search', (
        ('TEST1', {'heading': 'Online Audio', 'heading_type': 'genre' }),
        ('TEST2', {'heading': 'Online Video', 'heading_type': 'genre' }),
        ('TEST3', {'heading': 'Online audio thing', 'heading_type': 'genre' }),
        ('TEST4', {'heading': 'Only something', 'heading_type': 'genre' }),
        ('TEST5', {'heading': 'Something else', 'heading_type': 'genre' }),
     ), 'search="audio thing" online', ['TEST3']),
    },

    # BROWSE QUERY BEHAVIOR
    { 'browse: partial matching via truncation': ('browse', (
        ('TEST1', {'heading': 'Online Audio', 'heading_type': 'genre' }),
        ('TEST2', {'heading': 'Online Video', 'heading_type': 'genre' }),
        ('TEST3', {'heading': 'Online audio thing', 'heading_type': 'genre' }),
        ('TEST4', {'heading': 'Only something', 'heading_type': 'genre' }),
        ('TEST5', {'heading': 'Something else', 'heading_type': 'genre' }),
     ), 'search=onl', ['TEST1', 'TEST2', 'TEST3', 'TEST4']),
    }, { 'browse: matching on first word': ('browse', (
        ('TEST1', {'heading': 'Online Audio', 'heading_type': 'genre' }),
        ('TEST2', {'heading': 'Online Video', 'heading_type': 'genre' }),
        ('TEST3', {'heading': 'Online audio thing', 'heading_type': 'genre' }),
        ('TEST4', {'heading': 'Only something', 'heading_type': 'genre' }),
        ('TEST5', {'heading': 'Something online', 'heading_type': 'genre' }),
     ), 'search=online', ['TEST1', 'TEST2', 'TEST3']),
    }, { 'browse: matching a partial phrase, no quotes': ('browse', (
        ('TEST1', {'heading': 'Online Audio', 'heading_type': 'genre' }),
        ('TEST2', {'heading': 'Online Video', 'heading_type': 'genre' }),
        ('TEST3', {'heading': 'Online audio thing', 'heading_type': 'genre' }),
        ('TEST4', {'heading': 'Only something', 'heading_type': 'genre' }),
        ('TEST5', {'heading': 'Something else', 'heading_type': 'genre' }),
     ), 'search=online aud', ['TEST1', 'TEST3']),
    }, { 'browse: matching a partial phrase w/one quote': ('browse', (
        ('TEST1', {'heading': 'Online Audio', 'heading_type': 'genre' }),
        ('TEST2', {'heading': 'Online Video', 'heading_type': 'genre' }),
        ('TEST3', {'heading': 'Online audio thing', 'heading_type': 'genre' }),
        ('TEST4', {'heading': 'Only something', 'heading_type': 'genre' }),
        ('TEST5', {'heading': 'Something else', 'heading_type': 'genre' }),
     ), 'search="online aud', ['TEST1', 'TEST3']),
    }, { 'browse: full phrase in quotes negates truncation': ('browse', (
        ('TEST1', {'heading': 'Online Audio', 'heading_type': 'genre' }),
        ('TEST2', {'heading': 'Online Video', 'heading_type': 'genre' }),
        ('TEST3', {'heading': 'Online audio thing', 'heading_type': 'genre' }),
        ('TEST4', {'heading': 'Only something', 'heading_type': 'genre' }),
        ('TEST5', {'heading': 'Something else', 'heading_type': 'genre' }),
     ), 'search="online audio"', ['TEST1']),
    }, { 'browse: browse queries are left-anchored': ('browse', (
        ('TEST1', {'heading': 'Online Audio', 'heading_type': 'genre' }),
        ('TEST2', {'heading': 'Online Video', 'heading_type': 'genre' }),
        ('TEST3', {'heading': 'Online audio thing', 'heading_type': 'genre' }),
        ('TEST4', {'heading': 'Only something', 'heading_type': 'genre' }),
        ('TEST5', {'heading': 'Something else', 'heading_type': 'genre' }),
     ), 'search=audio', []),
    }, 
)


# HELPER FUNCTIONS for compiling test data into pytest parameters

def compile_params(parameters):
    """
    Compile a tuple of test parameters for pytest.parametrize, from one
    of the above PARAMETERS__* constants.
    """
    return tuple(p.values()[0] for p in parameters[1:])


def compile_ids(parameters):
    """
    Compile a tuple of test IDs for pytest.parametrize, from one of the
    above PARAMETERS__* constants.
    """
    return tuple(p.keys()[0] for p in parameters[1:])


# PYTEST FIXTURES

@pytest.fixture
def api_settings(settings):
    """
    Pytest fixture that sets a few default Django settings for the API
    tests in this module. Returns the `settings` object. Doing setup
    like this here via a fixture seems slightly better than putting
    this in the `test` settings module--the relevant settings are
    closer to the tests that use them. Just have to make sure to
    include this fixture in all of the tests that need them.
    """
    settings.REST_FRAMEWORK['PAGINATE_BY_PARAM'] = 'limit'
    settings.REST_FRAMEWORK['PAGINATE_PARAM'] = 'offset'
    settings.REST_FRAMEWORK['SEARCH_PARAM'] = 'search'
    settings.REST_FRAMEWORK['SEARCHTYPE_PARAM'] = 'searchtype'
    settings.REST_FRAMEWORK['MAX_PAGINATE_BY'] = 500
    settings.REST_FRAMEWORK['PAGINATE_BY'] = 500
    return settings


# TESTS
# ---------------------------------------------------------------------

@pytest.mark.parametrize('rtype', ['search', 'browse'])
def test_resources(rtype, api_settings, bl_solr_env, api_client,
                   assert_obj_fields_match_serializer):
    """
    The asm*Suggestions resources should should have a list view, with
    objects available in an "_embedded" object. Data objects should
    have fields matching the associated view serializer's `fields`
    attribute.
    """
    url = '{}{}/'.format(API_ROOT, RESOURCE_TYPES[rtype]['url'])
    response = api_client.get(url)
    objects = response.data['_embedded'][RESOURCE_TYPES[rtype]['resource']]
    serializer = response.renderer_context['view'].get_serializer()
    assert_obj_fields_match_serializer(objects[0], serializer)


@pytest.mark.parametrize('rtype, test_data, search, expected',
                         compile_params(PARAMETERS__FILTER_TESTS),
                         ids=compile_ids(PARAMETERS__FILTER_TESTS))
def test_list_view_filters(rtype, test_data, search, expected,
                           api_settings, assemble_bl_test_records, api_client,
                           get_found_ids, do_filter_search):
    """
    Given the provided `test_data` records: requesting the given
    `resource` using the provided search filter parameters (`search`)
    should return each of the records in `expected` and NONE of the
    records NOT in `expected`.
    """
    test_ids = set([r[0] for r in test_data])
    expected_ids = set(expected) if expected is not None else set()
    not_expected_ids = test_ids - expected_ids

    # NOTE: Because suggestion data is kept small, the main identifying
    # field is heading_display (or headingDisplay).
    erecs, trecs = assemble_bl_test_records(test_data, 'blsuggest',
                                            id_field='heading_display')

    url = '{}{}/'.format(API_ROOT, RESOURCE_TYPES[rtype]['url'])
    response = do_filter_search(url, search, api_client)
    max_found = api_settings.REST_FRAMEWORK['PAGINATE_BY'] - 1
    found_ids = set(get_found_ids('headingDisplay', response, max_found))
    assert all([i in found_ids for i in expected_ids])
    assert all([i not in found_ids for i in not_expected_ids])

