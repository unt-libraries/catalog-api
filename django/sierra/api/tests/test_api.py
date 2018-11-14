"""
Contains integration tests for the `api` app.
"""

import pytest

from . import solr_test_profiles as tp

# FIXTURES AND TEST DATA
# External fixtures used below can be found in
# django/sierra/conftest.py:
#
# django/sierra/api/tests/conftest.py:
#     api_solr_env
#     api_client


# TESTS


@pytest.mark.parametrize('url, err_text', [
    ('/api/v1/items/?dueDate[gt]=2018', 'datetime was formatted incorrectly'),
    ('/api/v1/items/?recordNumber[invalid]=i10000100', 'not a valid operator'),
    ('/api/v1/items/?recordNumber[in]=i10000100', 'require an array'),
    ('/api/v1/items/?recordNumber[range]=i10000100', 'require an array'),
    ('/api/v1/items/?nonExistent=0', 'not a valid field for filtering'),
    ('/api/v1/items/?orderBy=nonExistent', 'not a valid field for ordering'),

])
def test_request_error_badquery(url, err_text, api_solr_env, api_client):
    """
    Requesting from the given URL should result in a 400 error response
    (due to a bad query), which contains the given error text.
    """
    response = api_client.get(url)
    assert response.status_code == 400
    assert 'Query filter criteria' in response.data['detail']
    assert err_text in response.data['detail']


@pytest.mark.parametrize('resource, solrtype, default_limit, max_limit, '
                         'limit, offset, exp_results, exp_start, exp_end, '
                         'exp_prev_offset, exp_next_offset', [
    ('items', 'item', 20, 50, None, None, 20, 0, 19, None, 20),
    ('items', 'item', 20, 50, 20, None, 20, 0, 19, None, 20),
    ('items', 'item', 20, 50, None, 0, 20, 0, 19, None, 20),
    ('items', 'item', 20, 50, 20, 0, 20, 0, 19, None, 20),
    ('items', 'item', 20, 50, 20, 1, 20, 1, 20, 0, 21),
    ('items', 'item', 20, 50, 20, 20, 20, 20, 39, 0, 40),
    ('items', 'item', 20, 50, 20, 40, 20, 40, 59, 20, 60),
    ('items', 'item', 20, 50, 25, 20, 25, 20, 44, 0, 45),
    ('items', 'item', 20, 50, 20, 180, 20, 180, 199, 160, None),
    ('items', 'item', 20, 50, 20, 190, 10, 190, 199, 170, None),
    ('items', 'item', 20, 50, 0, None, 0, 0, -1, None, 0),
    ('items', 'item', 20, 50, 50, None, 50, 0, 49, None, 50),
    ('items', 'item', 20, 50, 51, None, 50, 0, 49, None, 50),
    ('items', 'item', 20, 300, 300, None, 200, 0, 199, None, None),
    ('items', 'item', 20, 50, 20, 300, 0, 300, 199, 280, None),
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
def test_list_view_pagination(resource, solrtype, default_limit, max_limit,
                              limit, offset, exp_results, exp_start, exp_end,
                              exp_prev_offset, exp_next_offset, settings,
                              api_solr_env, api_client):
    """
    Requesting the resource from the the given URL using the provided
    limit and offset parameters should result in a data structure that
    we can paginate through in predictable ways.
    """
    settings.REST_FRAMEWORK['PAGINATE_BY'] = default_limit
    settings.REST_FRAMEWORK['MAX_PAGINATE_BY'] = max_limit
    settings.REST_FRAMEWORK['PAGINATE_BY_PARAM'] = 'limit'
    settings.REST_FRAMEWORK['PAGINATE_PARAM'] = 'offset'
    exp_total = len(api_solr_env.records[solrtype])

    base_url = '/api/v1/{}/'.format(resource)
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

