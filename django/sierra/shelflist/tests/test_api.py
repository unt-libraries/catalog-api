"""
Tests API features applicable to the `shelflist` app.
"""

import pytest
from datetime import datetime

# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in ...
# django/sierra/base/tests/conftest.py:

# API_ROOT: Base URL for the API we're testing.
API_ROOT = '/api/v1/'


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
    'test_data, search, expected',

    # Filter by Call Number
    { 'exact call number | one match': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1', 'call_number_type': 'lc'}),
     ), 'callNumber=AB100 .A1 1', ['TEST1']),
    }, { 'exact call number, truncated | no matches': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1', 'call_number_type': 'lc'}),
     ), 'callNumber=AB100 .A1', []),
    }, { 'exact call number, normalized | one match': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1', 'call_number_type': 'lc'}),
     ), 'callNumber=ab100a11', ['TEST1']),
    }, { 'exact call number, normalized and truncated | no matches': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1', 'call_number_type': 'lc'}),
     ), 'callNumber=ab100a1', []),
    }, { 'startswith call number | no matches': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1', 'call_number_type': 'lc'}),
     ), 'callNumber[startswith]=AB102', []),
    }, { 'startswith call number | multiple matches': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1', 'call_number_type': 'lc'}),
     ), 'callNumber[startswith]=AB1', ['TEST1', 'TEST2']),
    }, { 'startswith call number, extra spaces | one match': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1', 'call_number_type': 'lc'}),
     ), 'callNumber[startswith]=AB 100 .A1', ['TEST1']),
    },

    # Filter by Call Number and Type
    { 'call number is correct; type is incorrect | no matches': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1', 'call_number_type': 'lc'}),
     ), 'callNumber=AB 100 .A1 1&callNumberType=other', []),
    }, { 'call number is correct; type is correct | one match': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1', 'call_number_type': 'lc'}),
     ), 'callNumber=AB 100 .A1 1&callNumberType=lc', ['TEST1']),
    },

    # Filter by Barcode
    { 'exact barcode | one match': ((
        ('TEST1', {'barcode': '5555000001'}),
        ('TEST2', {'barcode': '5555000002'}),
     ), 'barcode=5555000001', ['TEST1']),
    }, { 'exact barcode, truncated | no matches': ((
        ('TEST1', {'barcode': '5555000001'}),
        ('TEST2', {'barcode': '5555000002'}),
     ), 'barcode=555500000', []),
    }, { 'startswith barcode, truncated | one match': ((
        ('TEST1', {'barcode': '5555000001'}),
        ('TEST2', {'barcode': '5554000001'}),
     ), 'barcode[startswith]=5555', ['TEST1']),
    }, { 'startswith barcode, truncated | multiple matches': ((
        ('TEST1', {'barcode': '5555000001'}),
        ('TEST2', {'barcode': '5554000001'}),
     ), 'barcode[startswith]=555', ['TEST1', 'TEST2']),
    },

    # Filter by Item Status and Due Date
    { 'status CHECKED OUT | one match': ((
        ('TEST1', {'status_code': '-',
                   'due_date': datetime(2019, 6, 30, 00, 00, 00)}),
        ('TEST2', {'status_code': '-', 'due_date': None}),
     ), 'status_code=-&dueDate[isnull]=false', ['TEST1']),
    }, { 'status CHECKED OUT and status code a | one match': ((
        ('TEST1', {'status_code': 'a',
                   'due_date': datetime(2019, 6, 30, 00, 00, 00)}),
        ('TEST2', {'status_code': '-', 'due_date': None}),
     ), 'status_code=a&dueDate[isnull]=false', ['TEST1']),
    }, { 'status CHECKED OUT and status code a, b | multiple matches': ((
        ('TEST1', {'status_code': 'a',
                   'due_date': datetime(2019, 6, 30, 00, 00, 00)}),
        ('TEST2', {'status_code': 'b', 'due_date': None}),
        ('TEST3', {'status_code': 'b',
                   'due_date': datetime(2019, 9, 30, 00, 00, 00)}),
        ('TEST4', {'status_code': '-', 'due_date': None}),
     ), 'status_code[in]=[a,b]&dueDate[isnull]=false', ['TEST1', 'TEST3']),
    }, { 'status CHECKED OUT and status code a | no matches': ((
        ('TEST1', {'status_code': '-',
                   'due_date': datetime(2019, 6, 30, 00, 00, 00)}),
        ('TEST2', {'status_code': '-', 'due_date': None}),
     ), 'status_code=a&dueDate[isnull]=false', []),
    }, { 'status NOT CHECKED OUT and status code - | one match': ((
        ('TEST1', {'status_code': '-',
                   'due_date': datetime(2019, 6, 30, 00, 00, 00)}),
        ('TEST2', {'status_code': '-', 'due_date': None}),
     ), 'status_code=-&dueDate[isnull]=true', ['TEST2']),
    }, { 'status NOT CHECKED OUT and status code a | one match': ((
        ('TEST1', {'status_code': '-',
                   'due_date': datetime(2019, 6, 30, 00, 00, 00)}),
        ('TEST2', {'status_code': 'a', 'due_date': None}),
     ), 'status_code=a&dueDate[isnull]=true', ['TEST2']),
    }, { 'status NOT CHECKED OUT and status code a, b | multiple matches': ((
        ('TEST1', {'status_code': 'b',
                   'due_date': datetime(2019, 6, 30, 00, 00, 00)}),
        ('TEST2', {'status_code': 'a', 'due_date': None}),
        ('TEST3', {'status_code': 'b', 'due_date': None}),
     ), 'status_code[in]=[a,b]&dueDate[isnull]=true', ['TEST2', 'TEST3']),
    },

    # Filter by Suppression
    { 'suppression | one match': ((
        ('TEST1', {'suppressed': True}),
        ('TEST2', {'suppressed': False}),
     ), 'suppressed=true', ['TEST1']),
    }, { 'suppression | no matches': ((
        ('TEST1', {'suppressed': True}),
        ('TEST2', {'suppressed': True}),
     ), 'suppressed=false', []),
    }, { 'suppression | multiple matches': ((
        ('TEST1', {'suppressed': True}),
        ('TEST2', {'suppressed': True}),
     ), 'suppressed=true', ['TEST1', 'TEST2']),
    }, 

    # Filter by Shelf / Inventory Status
    { 'shelf status, on shelf | one match': ((
        ('TEST1', {'shelf_status': 'onShelf'}),
        ('TEST2', {'shelf_status': 'unknown'}),
     ), 'shelfStatus=onShelf', ['TEST1']),
    }, { 'shelf status, on shelf | no matches': ((
        ('TEST1', {'shelf_status': 'unknown'}),
        ('TEST2', {'shelf_status': 'unknown'}),
     ), 'shelfStatus=onShelf', []),
    }, { 'shelf status, on shelf or unknown | multiple matches': ((
        ('TEST1', {'shelf_status': 'onShelf'}),
        ('TEST2', {'shelf_status': 'unknown'}),
        ('TEST3', {'shelf_status': 'notOnShelf'}),
     ), 'shelfStatus[in]=[unknown,onShelf]', ['TEST1', 'TEST2']),
    },
    
    # Filter by Inventory Notes
    { 'inventory notes, attempt to keyword search | no matches': ((
        ('TEST1', {'inventory_notes': ['2019-01-01T20:49:49|user|my note']}),
        ('TEST2', {'inventory_notes': ['2019-01-01T20:49:49|@SYS|note text']}),
     ), 'inventoryNotes=user', []),
    }, { 'contains inventory notes | one note matches': ((
        ('TEST1', {'inventory_notes': ['2019-01-01T20:49:49|user|my note',
                                       '2019-03-01T00:00:00|@SYS|note text']}),
        ('TEST2', {'inventory_notes': ['2019-01-01T20:49:49|@SYS|note text']}),
     ), 'inventoryNotes[contains]=user', ['TEST1']),
    }, { 'contains inventory notes | no matches': ((
        ('TEST1', {'inventory_notes': ['2019-01-01T20:49:49|user|my note',
                                       '2019-03-01T00:00:00|@SYS|note text']}),
        ('TEST2', {'inventory_notes': ['2019-01-01T20:49:49|@SYS|note text']}),
     ), 'inventoryNotes[contains]=otheruser', []),
    }, { 'contains inventory notes | multiple matches': ((
        ('TEST1', {'inventory_notes': ['2019-01-01T20:49:49|user|my note',
                                       '2019-03-01T00:00:00|@SYS|note text']}),
        ('TEST2', {'inventory_notes': ['2019-01-01T20:49:49|@SYS|note text']}),
     ), 'inventoryNotes[contains]=note', ['TEST1', 'TEST2']),
    }, { 'inventory notes has user-entered notes | one match': ((
        ('TEST1', {'inventory_notes': ['2019-01-01T20:49:49|user|my note',
                                       '2019-03-01T00:00:00|@SYS|note text']}),
        ('TEST2', {'inventory_notes': ['2019-01-01T20:49:49|@SYS|note text']}),
     ), 'inventoryNotes[matches]=^[^|]*\\|[^@]', ['TEST1']),
    },

    # Filter by Flags
    { 'flags, filter by one flag | no matches': ((
        ('TEST1', {'flags': ['workflowStart']}),
        ('TEST2', {'flags': ['workflowStart', 'workflowOther']}),
     ), 'flags[in]=[workflowEnd]', []),
    }, { 'flags, filter by one flag | one match': ((
        ('TEST1', {'flags': ['workflowStart']}),
        ('TEST2', {'flags': ['workflowStart', 'workflowOther']}),
     ), 'flags[in]=[workflowOther]', ['TEST2']),
    }, { 'flags, filter by one flag | multiple matches': ((
        ('TEST1', {'flags': ['workflowStart']}),
        ('TEST2', {'flags': ['workflowStart', 'workflowOther']}),
     ), 'flags[in]=[workflowStart]', ['TEST1', 'TEST2']),
    }, { 'flags, filter by two flags | multiple matches (OR)': ((
        ('TEST1', {'flags': ['workflowStart', 'workflowEnd']}),
        ('TEST2', {'flags': ['workflowStart', 'workflowOther']}),
     ), 'flags[in]=[workflowOther,workflowEnd]', ['TEST1', 'TEST2']),
    }, { 'flags, filter by multiple flags | no matches': ((
        ('TEST1', {'flags': ['workflowStart', 'workflowEnd']}),
        ('TEST2', {'flags': ['workflowStart', 'workflowOther']}),
     ), 'flags[in]=[notHere1,notHere2,notHere3]', []),
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


@pytest.fixture
def get_shelflist_urls():
    """
    Pytest fixture. Given a list of shelflistitem solr records, returns
    a dict mapping location codes to URLs for each location shelflist.
    """
    def _get_shelflist_urls(records):
        locations = set([r['location_code'] for r in records])
        return { loc: ('{}locations/{}/shelflistitems/'.format(API_ROOT, loc)) 
                 for loc in locations }
    return _get_shelflist_urls


# TESTS
# ---------------------------------------------------------------------

def test_shelflistitem_resource(api_settings, get_shelflist_urls,
                                shelflist_solr_env, api_client,
                                pick_reference_object_having_link,
                                assert_obj_fields_match_serializer):
    """
    The shelflistitem resource should should have a list view and
    detail view; it should have objects available in an "_embedded"
    object in the list view, and accessing an object's "_links / self"
    URL should give you the same data object. Data objects should have
    fields matching the associated view serializer's `fields`
    attribute.
    """
    urls = get_shelflist_urls(shelflist_solr_env.records['shelflistitem'])
    list_resp = api_client.get(urls.values()[0])
    objects = list_resp.data['_embedded']['shelflistItems']
    ref_obj = pick_reference_object_having_link(objects, 'self')
    detail_resp = api_client.get(ref_obj['_links']['self']['href'])
    detail_obj = detail_resp.data
    assert ref_obj == detail_obj

    serializer = detail_resp.renderer_context['view'].get_serializer()
    assert_obj_fields_match_serializer(detail_obj, serializer)


@pytest.mark.parametrize('resource, linked_resource, link_field, '
                         'rev_link_field', [
    ('items', 'shelflistItems', 'shelflistItem', 'item'),
    ('shelflistItems', 'items', 'item', 'shelflistItem'),
    ('shelflistItems', 'locations', 'location', 'shelflist'),
    ('locations', 'shelflistItems', 'shelflist', 'location'),
])
def test_shelflistitem_links(resource, linked_resource, link_field,
                             rev_link_field, api_settings, api_client,
                             get_shelflist_urls, shelflist_solr_env,
                             pick_reference_object_having_link,
                             assert_obj_fields_match_serializer,
                             get_linked_view_and_objects):
    """
    Accessing linked resources (i.e. via the `_links` field in the JSON
    data) should return the expected resource(s).
    """
    if resource == 'shelflistItems':
        urls = get_shelflist_urls(shelflist_solr_env.records['shelflistitem'])
        url = urls.values()[0]
    else:
        url = '{}{}/'.format(API_ROOT, resource.lower())
    resp = api_client.get(url)
    objects = resp.data['_embedded'][resource]
    ref_obj = pick_reference_object_having_link(objects, link_field)
    lview, lobjs = get_linked_view_and_objects(api_client, ref_obj,
                                               link_field)
    assert lview.resource_name == linked_resource
    assert_obj_fields_match_serializer(lobjs[0], lview.get_serializer())
    _, rev_objs = get_linked_view_and_objects(api_client, lobjs[0],
                                              rev_link_field)
    assert ref_obj in rev_objs


@pytest.mark.parametrize('test_data, search, expected', 
                         compile_params(PARAMETERS__FILTER_TESTS),
                         ids=compile_ids(PARAMETERS__FILTER_TESTS))
def test_shelflistitem_list_view_filters(test_data, search, expected,
                                         api_settings, shelflist_solr_env,
                                         get_shelflist_urls,
                                         assemble_shelflist_test_records,
                                         api_client, get_found_ids,
                                         do_filter_search):
    """
    Given the provided `test_data` records: requesting the given
    `resource` using the provided search filter parameters (`search`)
    should return each of the records in `expected` and NONE of the
    records NOT in `expected`.
    """
    test_ids = set([r[0] for r in test_data])
    expected_ids = set(expected) if expected is not None else set()
    not_expected_ids = test_ids - expected_ids
    erecs = shelflist_solr_env.records['shelflistitem']
    loc = erecs[0]['location_code']
    loc_erecs = [r for r in erecs if r['location_code'] == loc]
    for test_id, data in test_data:
        data['location_code'] = loc
    _, trecs = assemble_shelflist_test_records(test_data,
                                               id_field='record_number')
    
    # First let's do a quick sanity check to make sure the resource
    # returns the correct num of records before the filter is applied.
    url = get_shelflist_urls(shelflist_solr_env.records['shelflistitem'])[loc]
    check_response = api_client.get(url)
    assert check_response.data['totalCount'] == len(loc_erecs) + len(trecs)

    response = do_filter_search(url, search, api_client)
    found_ids = set(get_found_ids('record_number', response))
    assert all([i in found_ids for i in expected_ids])
    assert all([i not in found_ids for i in not_expected_ids])


@pytest.mark.parametrize('order_by', [
    'callNumber',
    'id',
    'record_number',
    'barcode',
    'volume',
    'copyNumber',
    'rowNumber'
])
def test_shelflistitem_view_orderby(order_by, api_settings, shelflist_solr_env,
                                    get_shelflist_urls, api_client):
    """
    Attempting to order a shelflist view explicitly should always fail;
    the response should be a 400 error and a message stating that the
    order-by criteria is invalid.
    """
    sl_urls = get_shelflist_urls(shelflist_solr_env.records['shelflistitem'])
    test_url = '{}?orderBy={}'.format(sl_urls.values()[0], order_by)
    response = api_client.get(test_url)
    assert response.status_code == 400
    assert 'not a valid field for ordering' in response.data['detail']
