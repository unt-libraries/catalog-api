"""
Contains integration tests for the `api` app.
"""

from datetime import datetime
from pytz import utc

import pytest

from utils.test_helpers import solr_test_profiles as tp

# FIXTURES AND TEST DATA
# External fixtures used below can be found in
# django/sierra/conftest.py:
#
# django/sierra/api/tests/conftest.py:
#     api_solr_env
#     api_data_assembler
#     api_client


# API_ROOT: Base URL for the API we're testing.
API_ROOT = '/api/v1/'

# RESOURCE_METADATA: Lookup dict for mapping API resources to various
# test parameters.
RESOURCE_METADATA = {
    'bibs': { 'profile': 'bib', 'id_field': 'record_number' },
    'items': { 'profile': 'item', 'id_field': 'record_number' },
    'eresources': { 'profile': 'eresource', 'id_field': 'record_number' },
    'itemstatuses': { 'profile': 'itemstatus', 'id_field': 'code' },
    'itemtypes': { 'profile': 'itype', 'id_field': 'code' },
    'locations': { 'profile': 'location', 'id_field': 'code' }
}


# PARAMETERS__* constants contain parametrization data for certain
# tests. Each should be a tuple, where the first tuple member is a
# header string that describes the parametrization values (such as
# what you'd pass as the first arg to pytest.mark.parametrize); the
# others are single-entry dictionaries where the key is the parameter-
# list ID (such as what you'd pass to pytest.mark.parametrize via its
# `ids` kwarg) and the value is the list of parameters for that ID.

# PARAMETERS__FILTER_TESTS__NORMAL: Parameters for testing API filter
# behavior that works as expected. The provided `search` query string
# matches the `test_data` record(s) you'd expect it to match.
PARAMETERS__FILTER_TESTS__NORMAL = (
    'resource, test_data, search, expected',
    { 'exact text (bibs/creator) | no operator specified':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator=Person, Test A. 1900-', ['TEST1'])
    }, { 'exact text (bibs/creator) | one match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[exact]=Person, Test B. 1900-', ['TEST2']),
    }, { 'exact text (bibs/creator) | multiple matches':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test A. 1900-'}),
            ('TEST3', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[exact]=Person, Test A. 1900-', ['TEST1', 'TEST2']),
    }, { 'exact text (bibs/creator) | no matches':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[exact]=Person, Test C. 1900-', None),
    }, { 'exact text (bibs/creator) | negated, one match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[-exact]=Person, Test B. 1900-', ['TEST1']),
    }, { 'exact string (items/call_number) | one match':
        ('items', (
            ('TEST1', {'call_number': 'TEST CALLNUMBER 1'}),
            ('TEST2', {'call_number': 'TEST CALLNUMBER 2'}),
         ), 'callNumber[exact]=TEST CALLNUMBER 2', ['TEST2']),
    }, { 'exact string (items/call_number) | multiple matches':
        ('items', (
            ('TEST1', {'call_number': 'TEST CN 1'}),
            ('TEST2', {'call_number': 'TEST CN 2'}),
            ('TEST3', {'call_number': 'TEST CN 2'}),
            ('TEST4', {'call_number': 'TEST CN 1'}),
            ('TEST5', {'call_number': 'TEST CN 2'}),
         ), 'callNumber[exact]=TEST CN 2', ['TEST2', 'TEST3', 'TEST5']),
    }, { 'exact string (items/call_number) | no matches':
        ('items', (
            ('TEST1', {'call_number': 'TEST CALLNUMBER 1'}),
            ('TEST2', {'call_number': 'TEST CALLNUMBER 2'}),
         ), 'callNumber[exact]=TEST CALLNUMBER 3', None),
    }, { 'exact string (items/call_number) | negated, one match':
        ('items', (
            ('TEST1', {'call_number': 'TEST CALLNUMBER 1'}),
            ('TEST2', {'call_number': 'TEST CALLNUMBER 2'}),
         ), 'callNumber[-exact]=TEST CALLNUMBER 1', ['TEST2']),
    }, { 'exact int (items/copy_number) | one match':
        ('items', (
            ('TEST1', {'copy_number': '54'}),
            ('TEST2', {'copy_number': '12'}),
         ), 'copyNumber[exact]=54', ['TEST1']),
    }, { 'exact int (items/copy_number) | multiple matches':
        ('items', (
            ('TEST1', {'copy_number': '54'}),
            ('TEST2', {'copy_number': '12'}),
            ('TEST3', {'copy_number': '54'}),
            ('TEST4', {'copy_number': '12'}),
         ), 'copyNumber[exact]=54', ['TEST1', 'TEST3']),
    }, { 'exact int (items/copy_number) | no matches':
        ('items', (
            ('TEST1', {'copy_number': '54'}),
            ('TEST2', {'copy_number': '12'}),
         ), 'copyNumber[exact]=543', None),
    }, { 'exact int (items/copy_number) | negated, one match':
        ('items', (
            ('TEST1', {'copy_number': '54'}),
            ('TEST2', {'copy_number': '12'}),
         ), 'copyNumber[-exact]=54', ['TEST2']),
    }, { 'exact date (items/due_date) | one match':
        ('items', (
            ('TEST1', {'due_date': datetime(2018, 11, 30, 5, 0, 0,
                                            tzinfo=utc)}),
            ('TEST2', {'due_date': datetime(2018, 12, 13, 9, 0, 0,
                                            tzinfo=utc)}),
         ), 'dueDate[exact]=2018-11-30T05:00:00Z', ['TEST1']),
    }, { 'exact date (items/due_date) | multiple matches':
        ('items', (
            ('TEST1', {'due_date': datetime(2018, 11, 30, 5, 0, 0,
                                            tzinfo=utc)}),
            ('TEST2', {'due_date': datetime(2018, 11, 30, 5, 0, 0,
                                            tzinfo=utc)}),
            ('TEST3', {'due_date': datetime(2018, 12, 13, 9, 0, 0,
                                            tzinfo=utc)}),
         ), 'dueDate[exact]=2018-11-30T05:00:00Z', ['TEST1', 'TEST2']),
    }, { 'exact date (items/due_date) | no matches':
        ('items', (
            ('TEST1', {'due_date': datetime(2018, 11, 30, 5, 0, 0,
                                            tzinfo=utc)}),
            ('TEST2', {'due_date': datetime(2018, 12, 13, 9, 0, 0,
                                            tzinfo=utc)}),
         ), 'dueDate[exact]=1990-01-01T08:00:00Z', None),
    }, { 'exact date (items/due_date) | negated, one match':
        ('items', (
            ('TEST1', {'due_date': datetime(2018, 11, 30, 5, 0, 0,
                                            tzinfo=utc)}),
            ('TEST2', {'due_date': datetime(2018, 12, 13, 9, 0, 0,
                                            tzinfo=utc)}),
         ), 'dueDate[-exact]=2018-11-30T05:00:00Z', ['TEST2']),
    }, { 'exact bool (bibs/suppressed) | one match':
        ('items', (
            ('TEST1', {'suppressed': True}),
            ('TEST2', {'suppressed': False}),
         ), 'suppressed[exact]=true', ['TEST1']),
    }, { 'exact bool (bibs/suppressed) | multiple matches':
        ('items', (
            ('TEST1', {'suppressed': True}),
            ('TEST2', {'suppressed': False}),
            ('TEST3', {'suppressed': False}),
         ), 'suppressed[exact]=false', ['TEST2', 'TEST3']),
    }, { 'exact bool (bibs/suppressed) | no matches':
        ('items', (
            ('TEST1', {'suppressed': False}),
            ('TEST2', {'suppressed': False}),
         ), 'suppressed[exact]=true', None),
    }, { 'exact bool (bibs/suppressed) | negated, one match':
        ('items', (
            ('TEST1', {'suppressed': True}),
            ('TEST2', {'suppressed': False}),
         ), 'suppressed[-exact]=true', ['TEST2']),
    }, { 'contains text (bibs/creator) | one match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[contains]=A', ['TEST1']),
    }, { 'contains text (bibs/creator) | multiple matches':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
            ('TEST3', {'creator': 'Person, Test C. 2010-'}),
         ), 'creator[contains]=1900', ['TEST1', 'TEST2']),
    }, { 'contains text (bibs/creator) | no matches':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[contains]=Not Here', None),
    }, { 'contains text (bibs/creator) | negated, one match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[-contains]=A', ['TEST2']),
    },
)

# PARAMETERS__FILTER_TESTS__STRANGE: Parameters for testing "strange"
# API filter behavior. The provided `search` query string doesn't match
# the record(s) you'd normally expect, BUT the behavior IS logical (and
# expected) based on the current API filter implementation. As we work
# on filters, we should strive to convert these into passing normal
# tests--i.e., make them behave like you'd actually expect.
PARAMETERS__FILTER_TESTS__STRANGE = (
    'resource, test_data, search, expected',
    { 'STRANGE: exact text (bibs/creator) | works matching only one word':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[exact]=Test', ['TEST1', 'TEST2'])
    }, { 'STRANGE: contains text (bibs/creator) | fails on multiple words':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[contains]=Test A. 1900-', None),
    }, { 'STRANGE: contains text (bibs/creator) | fails with punctuation':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[contains]=A.', None),
    },
)


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
    return settings


# TESTS

@pytest.mark.parametrize('url, err_text', [
    ('/api/v1/items/?dueDate[gt]=2018', 'datetime was formatted incorrectly'),
    ('/api/v1/items/?recordNumber[invalid]=i10000100', 'not a valid operator'),
    ('/api/v1/items/?recordNumber[in]=i10000100', 'require an array'),
    ('/api/v1/items/?recordNumber[range]=i10000100', 'require an array'),
    ('/api/v1/items/?recordNumber=[i1,i2]', 'Arrays of values are only used'),
    ('/api/v1/items/?nonExistent=0', 'not a valid field for filtering'),
    ('/api/v1/items/?orderBy=nonExistent', 'not a valid field for ordering'),
    ('/api/v1/bibs/?searchtype=nonExistent', 'searchtype parameter must be'),
    ('/api/v1/bibs/?search=none:none', 'undefined field'),
    ('/api/v1/bibs/?suppressed=not', 'expected a boolean'),
    ('/api/v1/bibs/?recordNumber[isnull]=not', 'expected a boolean'),
])
def test_request_error_badquery(url, err_text, api_solr_env, api_client,
                                api_settings):
    """
    Requesting from the given URL should result in a 400 error response
    (due to a bad query), which contains the given error text.
    """
    response = api_client.get(url)
    assert response.status_code == 400
    assert err_text in response.data['detail']


@pytest.mark.parametrize('resource, default_limit, max_limit, limit, offset, '
                         'exp_results, exp_start, exp_end, exp_prev_offset, '
                         'exp_next_offset', [
    ('items', 20, 50, None, None, 20, 0, 19, None, 20),
    ('items', 20, 50, 20, None, 20, 0, 19, None, 20),
    ('items', 20, 50, None, 0, 20, 0, 19, None, 20),
    ('items', 20, 50, 20, 0, 20, 0, 19, None, 20),
    ('items', 20, 50, 20, 1, 20, 1, 20, 0, 21),
    ('items', 20, 50, 20, 20, 20, 20, 39, 0, 40),
    ('items', 20, 50, 20, 40, 20, 40, 59, 20, 60),
    ('items', 20, 50, 25, 20, 25, 20, 44, 0, 45),
    ('items', 20, 50, 20, 180, 20, 180, 199, 160, None),
    ('items', 20, 50, 20, 190, 10, 190, 199, 170, None),
    ('items', 20, 50, 0, None, 0, 0, -1, None, 0),
    ('items', 20, 50, 50, None, 50, 0, 49, None, 50),
    ('items', 20, 50, 51, None, 50, 0, 49, None, 50),
    ('items', 20, 300, 300, None, 200, 0, 199, None, None),
    ('items', 20, 50, 20, 300, 0, 300, 199, 280, None),
], ids=[
    'no limit or offset given => use defaults',
    'limit=default, no offset given => use defaults',
    'no limit given, offset=0 => use defaults',
    'limit=default and offset=0 => use defaults',
    'limit=20, offset=1 => 20 results, page offset by 1',
    'limit=20, offset=20 => 20 results, page offset by 20',
    'limit=20, offset=40 => 20 results, page offset by 40',
    'limit=25, offset=20 => 25 results, page offset by 20',
    'limit=20, offset=180 (total recs is 200) => 20 results, no next page',
    'limit=20, offset=190 (total recs is 200) => 10 results, no next page',
    'limit=0 => 0 results (STRANGE: endRow, next page)',
    'limit=max => max results',
    'limit > max => max results',
    'limit > total => total results, no next page',
    'offset > total => 0 results, no next page (STRANGE: startRow, prev page)'
])
def test_list_view_pagination(resource, default_limit, max_limit, limit,
                              offset, exp_results, exp_start, exp_end,
                              exp_prev_offset, exp_next_offset, api_settings,
                              api_solr_env, api_client):
    """
    Requesting the given resource using the provided limit and offset
    parameters should result in a data structure that we can paginate
    through in predictable ways.
    """
    api_settings.REST_FRAMEWORK['PAGINATE_BY'] = default_limit
    api_settings.REST_FRAMEWORK['MAX_PAGINATE_BY'] = max_limit
    profile = RESOURCE_METADATA[resource]['profile']
    exp_total = len(api_solr_env.records[profile])

    base_url = '{}{}/'.format(API_ROOT, resource)
    limitq = 'limit={}'.format(limit) if limit is not None else ''
    offsetq = 'offset={}'.format(offset) if offset is not None else ''
    qstring = '&'.join([part for part in (limitq, offsetq) if part])
    url = '?'.join([part for part in (base_url, qstring) if part])
    response = api_client.get(url)
    data = response.data
    self_link = data['_links']['self']['href']
    next_link = data['_links'].get('next', {'href': None})['href']
    prev_link = data['_links'].get('previous', {'href': None})['href']
    records = data.get('_embedded', {resource: []})[resource]

    assert response.status_code == 200
    assert len(records) == exp_results
    assert data['totalCount'] == exp_total
    assert data['startRow'] == exp_start
    assert data['endRow'] == exp_end

    assert self_link.endswith(url)

    if exp_next_offset is None:
        assert next_link is None
    else:
        assert limitq in next_link
        assert 'offset={}'.format(exp_next_offset) in next_link

    if exp_prev_offset is None:
        assert prev_link is None
    else:
        assert limitq in prev_link
        assert 'offset={}'.format(exp_prev_offset) in prev_link


@pytest.mark.parametrize('resource, test_data, search, expected',
                         compile_params(PARAMETERS__FILTER_TESTS__NORMAL) +
                         compile_params(PARAMETERS__FILTER_TESTS__STRANGE),
                         ids=compile_ids(PARAMETERS__FILTER_TESTS__NORMAL) +
                             compile_ids(PARAMETERS__FILTER_TESTS__STRANGE))
def test_list_view_filters(resource, test_data, search, expected, api_settings,
                           api_solr_env, api_data_assembler, api_client):
    """
    Given the provided `test_data` records: requesting the given
    `resource` using the provided search filter parameters (`search`)
    should return each of the records in `expected` and NONE of the
    records NOT in `expected`.
    """
    assembler = api_data_assembler
    gens = assembler.gen_factory
    profile = RESOURCE_METADATA[resource]['profile']
    solr_id_field = RESOURCE_METADATA[resource]['id_field']
    env_recs = api_solr_env.records[profile]
    test_recs = assembler.load_static_test_data(profile, test_data,
                                                solr_id_field, env_recs)
    test_ids = set([r[0] for r in test_data])
    expected_ids = set(expected) if expected is not None else set()
    not_expected_ids = test_ids - expected_ids

    api_settings.REST_FRAMEWORK['MAX_PAGINATE_BY'] = 500
    api_settings.REST_FRAMEWORK['PAGINATE_BY'] = 500

    # First let's do a quick sanity check to make sure the resource
    # returns the correct num of records before the filter is applied.
    check_response = api_client.get('{}{}/'.format(API_ROOT, resource))
    assert check_response.data['totalCount'] == len(env_recs) + len(test_recs)

    # Now the actual filter test.
    response = api_client.get('{}{}/?{}'.format(API_ROOT, resource, search))
    serializer = response.renderer_context['view'].get_serializer()
    api_id_field = serializer.render_field_name(solr_id_field)
    total_found = response.data['totalCount']
    data = response.data.get('_embedded', {resource: []})[resource]
    found_ids = set([r[api_id_field] for r in data])

    # FAIL if we've returned any data not on this page of results.
    assert len(data) == total_found    
    assert all([i in found_ids for i in expected_ids])
    assert all([i not in found_ids for i in not_expected_ids])
