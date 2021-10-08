"""
Tests API features applicable to the `shelflist` app.
"""

from __future__ import absolute_import
from __future__ import print_function
import pytest
import ujson
import jsonpatch
from datetime import datetime

from six import text_type
from six.moves import range

from shelflist.exporters import ItemsToSolr
from shelflist.search_indexes import ShelflistItemIndex
from shelflist.serializers import ShelflistItemSerializer


# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in ...
# django/sierra/base/tests/conftest.py:

# API_ROOT: Base URL for the API we're testing.
API_ROOT = '/api/v1/'


REDIS_SHELFLIST_PREFIX = ItemsToSolr.redis_shelflist_prefix


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
    {'exact call number | one match': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1', 'call_number_type': 'lc'}),
    ), 'callNumber=AB100 .A1 1', ['TEST1']),
    }, {'exact call number, truncated | no matches': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1', 'call_number_type': 'lc'}),
    ), 'callNumber=AB100 .A1', []),
    }, {'exact call number, normalized | one match': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1', 'call_number_type': 'lc'}),
    ), 'callNumber=ab100a11', ['TEST1']),
    }, {'exact call number, normalized and truncated | no matches': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1', 'call_number_type': 'lc'}),
    ), 'callNumber=ab100a1', []),
    }, {'startswith call number | no matches': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1',
                   'call_number_type': 'lc'}),
    ), 'callNumber[startswith]=AB102', []),
    }, {'startswith call number | multiple matches': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1',
                   'call_number_type': 'lc'}),
    ), 'callNumber[startswith]=AB1', ['TEST1', 'TEST2']),
    }, {'startswith call number, extra spaces | one match': ((
        ('TEST1', {'call_number': 'AB100 .A1 1',
                   'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1',
                   'call_number_type': 'lc'}),
    ), 'callNumber[startswith]=AB 100 .A1', ['TEST1']),
    },

    # Filter by Call Number and Type
    {'call number is correct; type is incorrect | no matches': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1', 'call_number_type': 'lc'}),
    ), 'callNumber=AB 100 .A1 1&callNumberType=other', []),
    }, {'call number is correct; type is correct | one match': ((
        ('TEST1', {'call_number': 'AB100 .A1 1', 'call_number_type': 'lc'}),
        ('TEST2', {'call_number': 'AB101 .A1 1', 'call_number_type': 'lc'}),
    ), 'callNumber=AB 100 .A1 1&callNumberType=lc', ['TEST1']),
    },

    # Filter by Barcode
    {'exact barcode | one match': ((
        ('TEST1', {'barcode': '5555000001'}),
        ('TEST2', {'barcode': '5555000002'}),
    ), 'barcode=5555000001', ['TEST1']),
    }, {'exact barcode, truncated | no matches': ((
        ('TEST1', {'barcode': '5555000001'}),
        ('TEST2', {'barcode': '5555000002'}),
    ), 'barcode=555500000', []),
    }, {'startswith barcode, truncated | one match': ((
        ('TEST1', {'barcode': '5555000001'}),
        ('TEST2', {'barcode': '5554000001'}),
    ), 'barcode[startswith]=5555', ['TEST1']),
    }, {'startswith barcode, truncated | multiple matches': ((
        ('TEST1', {'barcode': '5555000001'}),
        ('TEST2', {'barcode': '5554000001'}),
    ), 'barcode[startswith]=555', ['TEST1', 'TEST2']),
    },

    # Filter by Item Status and Due Date
    {'status CHECKED OUT | one match': ((
        ('TEST1', {'status_code': '-',
                   'due_date': datetime(2019, 6, 30, 00, 00, 00)}),
        ('TEST2', {'status_code': '-', 'due_date': None}),
    ), 'status_code=-&dueDate[isnull]=false', ['TEST1']),
    }, {'status CHECKED OUT and status code a | one match': ((
        ('TEST1', {'status_code': 'a',
                   'due_date': datetime(2019, 6, 30, 00, 00, 00)}),
        ('TEST2', {'status_code': '-', 'due_date': None}),
    ), 'status_code=a&dueDate[isnull]=false', ['TEST1']),
    }, {'status CHECKED OUT and status code a, b | multiple matches': ((
        ('TEST1', {'status_code': 'a',
                   'due_date': datetime(2019, 6, 30, 00, 00, 00)}),
        ('TEST2', {'status_code': 'b', 'due_date': None}),
        ('TEST3', {'status_code': 'b',
                   'due_date': datetime(2019, 9, 30, 00, 00, 00)}),
        ('TEST4', {'status_code': '-', 'due_date': None}),
    ), 'status_code[in]=[a,b]&dueDate[isnull]=false', ['TEST1', 'TEST3']),
    }, {'status CHECKED OUT and status code a | no matches': ((
        ('TEST1', {'status_code': '-',
                   'due_date': datetime(2019, 6, 30, 00, 00, 00)}),
        ('TEST2', {'status_code': '-', 'due_date': None}),
    ), 'status_code=a&dueDate[isnull]=false', []),
    }, {'status NOT CHECKED OUT and status code - | one match': ((
        ('TEST1', {'status_code': '-',
                   'due_date': datetime(2019, 6, 30, 00, 00, 00)}),
        ('TEST2', {'status_code': '-', 'due_date': None}),
    ), 'status_code=-&dueDate[isnull]=true', ['TEST2']),
    }, {'status NOT CHECKED OUT and status code a | one match': ((
        ('TEST1', {'status_code': '-',
                   'due_date': datetime(2019, 6, 30, 00, 00, 00)}),
        ('TEST2', {'status_code': 'a', 'due_date': None}),
    ), 'status_code=a&dueDate[isnull]=true', ['TEST2']),
    }, {'status NOT CHECKED OUT and status code a, b | multiple matches': ((
        ('TEST1', {'status_code': 'b',
                   'due_date': datetime(2019, 6, 30, 00, 00, 00)}),
        ('TEST2', {'status_code': 'a', 'due_date': None}),
        ('TEST3', {'status_code': 'b', 'due_date': None}),
    ), 'status_code[in]=[a,b]&dueDate[isnull]=true', ['TEST2', 'TEST3']),
    },

    # Filter by Suppression
    {'suppression | one match': ((
        ('TEST1', {'suppressed': True}),
        ('TEST2', {'suppressed': False}),
    ), 'suppressed=true', ['TEST1']),
    }, {'suppression | no matches': ((
        ('TEST1', {'suppressed': True}),
        ('TEST2', {'suppressed': True}),
    ), 'suppressed=false', []),
    }, {'suppression | multiple matches': ((
        ('TEST1', {'suppressed': True}),
        ('TEST2', {'suppressed': True}),
    ), 'suppressed=true', ['TEST1', 'TEST2']),
    },

    # Filter by Shelf / Inventory Status
    {'shelf status, on shelf | one match': ((
        ('TEST1', {'shelf_status': 'onShelf'}),
        ('TEST2', {'shelf_status': 'unknown'}),
    ), 'shelfStatus=onShelf', ['TEST1']),
    }, {'shelf status, on shelf | no matches': ((
        ('TEST1', {'shelf_status': 'unknown'}),
        ('TEST2', {'shelf_status': 'unknown'}),
    ), 'shelfStatus=onShelf', []),
    }, {'shelf status, on shelf or unknown | multiple matches': ((
        ('TEST1', {'shelf_status': 'onShelf'}),
        ('TEST2', {'shelf_status': 'unknown'}),
        ('TEST3', {'shelf_status': 'notOnShelf'}),
    ), 'shelfStatus[in]=[unknown,onShelf]', ['TEST1', 'TEST2']),
    },

    # Filter by Inventory Notes
    {'inventory notes, attempt to keyword search | no matches': ((
        ('TEST1', {'inventory_notes': ['2019-01-01T20:49:49|user|my note']}),
        ('TEST2', {'inventory_notes': ['2019-01-01T20:49:49|@SYS|note text']}),
    ), 'inventoryNotes=user', []),
    }, {'contains inventory notes | one note matches': ((
        ('TEST1', {'inventory_notes': ['2019-01-01T20:49:49|user|my note',
                                       '2019-03-01T00:00:00|@SYS|note text']}),
        ('TEST2', {'inventory_notes': [
            '2019-01-01T20:49:49|@SYS|note text']}),
    ), 'inventoryNotes[contains]=user', ['TEST1']),
    }, {'contains inventory notes | no matches': ((
        ('TEST1', {'inventory_notes': ['2019-01-01T20:49:49|user|my note',
                                       '2019-03-01T00:00:00|@SYS|note text']}),
        ('TEST2', {'inventory_notes': [
            '2019-01-01T20:49:49|@SYS|note text']}),
    ), 'inventoryNotes[contains]=otheruser', []),
    }, {'contains inventory notes | multiple matches': ((
        ('TEST1', {'inventory_notes': ['2019-01-01T20:49:49|user|my note',
                                       '2019-03-01T00:00:00|@SYS|note text']}),
        ('TEST2', {'inventory_notes': [
            '2019-01-01T20:49:49|@SYS|note text']}),
    ), 'inventoryNotes[contains]=note', ['TEST1', 'TEST2']),
    }, {'inventory notes has user-entered notes | one match': ((
        ('TEST1', {'inventory_notes': ['2019-01-01T20:49:49|user|my note',
                                       '2019-03-01T00:00:00|@SYS|note text']}),
        ('TEST2', {'inventory_notes': [
            '2019-01-01T20:49:49|@SYS|note text']}),
    ), 'inventoryNotes[matches]=^[^|]*\\|[^@]', ['TEST1']),
    },

    # Filter by Flags
    {'flags, filter by one flag | no matches': ((
        ('TEST1', {'flags': ['workflowStart']}),
        ('TEST2', {'flags': ['workflowStart', 'workflowOther']}),
    ), 'flags[in]=[workflowEnd]', []),
    }, {'flags, filter by one flag | one match': ((
        ('TEST1', {'flags': ['workflowStart']}),
        ('TEST2', {'flags': ['workflowStart', 'workflowOther']}),
    ), 'flags[in]=[workflowOther]', ['TEST2']),
    }, {'flags, filter by one flag | multiple matches': ((
        ('TEST1', {'flags': ['workflowStart']}),
        ('TEST2', {'flags': ['workflowStart', 'workflowOther']}),
    ), 'flags[in]=[workflowStart]', ['TEST1', 'TEST2']),
    }, {'flags, filter by two flags | multiple matches (OR)': ((
        ('TEST1', {'flags': ['workflowStart', 'workflowEnd']}),
        ('TEST2', {'flags': ['workflowStart', 'workflowOther']}),
    ), 'flags[in]=[workflowOther,workflowEnd]', ['TEST1', 'TEST2']),
    }, {'flags, filter by multiple flags | no matches': ((
        ('TEST1', {'flags': ['workflowStart', 'workflowEnd']}),
        ('TEST2', {'flags': ['workflowStart', 'workflowOther']}),
    ), 'flags[in]=[notHere1,notHere2,notHere3]', []),
    },
)


# TESTDATA__FIRSTITEMPERLOCATION: We use a consistent set of test data
# for testing the firstitemperlocation resource.
TESTDATA__FIRSTITEMPERLOCATION = (
    ('atest1', 1,
        {'location_code': 'atest',
         'barcode': '1',
         'call_number': 'BB 1234 C35 1990',
         'call_number_type': 'lc'}),
    ('atest2', 0,
        {'location_code': 'atest',
         'barcode': '2',
         'call_number': 'BB 1234 A22 2000',
         'call_number_type': 'lc'}),
    ('atest3', 2,
        {'location_code': 'atest',
         'barcode': '3',
         'call_number': 'BC 2345 F80',
         'call_number_type': 'lc'}),
    ('atest4', 3,
        {'location_code': 'atest',
         'barcode': '4',
         'call_number': 'BB 1234',
         'call_number_type': 'sudoc'}),
    ('btest1', 0,
        {'location_code': 'btest',
         'barcode': '3',
         'call_number': 'BB 1234 D99',
         'call_number_type': 'lc'}),
    ('btest2', 3,
        {'location_code': 'btest',
         'barcode': '4',
         'call_number': 'BB 1234 A22',
         'call_number_type': 'sudoc'}),
    ('btest3', 1,
        {'location_code': 'btest',
         'barcode': '5',
         'call_number': 'CC 9876 H43',
         'call_number_type': 'lc'}),
    ('btest4', 2,
        {'location_code': 'btest',
         'barcode': '6',
         'call_number': 'BB 1234',
         'call_number_type': 'sudoc'}),
    ('ctest1', 1,
        {'location_code': 'ctest',
         'barcode': '8',
         'call_number': 'BB 1234 D99 2016',
         'call_number_type': 'lc'}),
    ('ctest2', 3,
        {'location_code': 'ctest',
         'barcode': '9',
         'call_number': 'CC 1234 A22',
         'call_number_type': 'other'}),
    ('ctest3', 0,
        {'location_code': 'ctest',
         'barcode': '10',
         'call_number': '900.1 H43',
         'call_number_type': 'dewey'}),
    ('ctest4', 2,
        {'location_code': 'ctest',
         'barcode': '11',
         'call_number': 'AB 1234',
         'call_number_type': 'other'}),
)


PARAMETERS__FIRSTITEMPERLOCATION = (
    ('test_data, search, expected'),
    {'LC call number type | A match at each location':
        (TESTDATA__FIRSTITEMPERLOCATION,
         'callNumber[startswith]=BB 12&callNumberType=lc',
         ['atest2', 'btest1', 'ctest1']),
     }, {'LC call number type | A match at one location':
         (TESTDATA__FIRSTITEMPERLOCATION,
          'callNumber[startswith]=BC&callNumberType=lc',
          ['atest3']),
         }, {'LC call number type | No matches':
             (TESTDATA__FIRSTITEMPERLOCATION,
              'callNumber[startswith]=D&callNumberType=lc',
              None),
             }, {'SUDOC call number type | A match at two locations':
                 (TESTDATA__FIRSTITEMPERLOCATION,
                  'callNumber[startswith]=BB&callNumberType=sudoc',
                  ['atest4', 'btest4']),
                 }, {'DEWEY call number type | A match at one location':
                     (TESTDATA__FIRSTITEMPERLOCATION,
                      'callNumber[startswith]=900&callNumberType=dewey',
                      ['ctest3']),
                     }, {'OTHER call number type | A match at one location':
                         (TESTDATA__FIRSTITEMPERLOCATION,
                          'callNumber[startswith]=C&callNumberType=other',
                          ['ctest2']),
                         }, {'BARCODE | A match at two locations':
                             (TESTDATA__FIRSTITEMPERLOCATION,
                              'barcode=3',
                              ['atest3', 'btest1']),
                             },
)


# HELPER FUNCTIONS for compiling test data into pytest parameters

def compile_params(parameters):
    """
    Compile a tuple of test parameters for pytest.parametrize, from one
    of the above PARAMETERS__* constants.
    """
    return tuple(list(p.values())[0] for p in parameters[1:])


def compile_ids(parameters):
    """
    Compile a tuple of test IDs for pytest.parametrize, from one of the
    above PARAMETERS__* constants.
    """
    return tuple(list(p.keys())[0] for p in parameters[1:])


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
        return {loc: ('{}locations/{}/shelflistitems/'.format(API_ROOT, loc))
                for loc in locations}
    return _get_shelflist_urls


@pytest.fixture
def assemble_custom_shelflist(assemble_shelflist_test_records):
    """
    Pytest fixture. Returns a utility function for creating a custom
    shelflist at the given location code. Uses the
    `assemble_shelflist_test_records` fixture to add the records to the
    active Solr environment for the duration of the test. Returns a
    tuple: environment records (erecs), location records (lrecs), and
    item test records (trecs).
    """
    def _assemble_custom_shelflist(lcode, sl_item_data, id_field='id'):
        test_locdata = [(lcode, {})]
        test_itemdata = []
        for item_id, data in sl_item_data:
            new_data = data.copy()
            new_data['location_code'] = lcode
            test_itemdata.append((item_id, new_data))
        _, lrecs = assemble_shelflist_test_records(test_locdata,
                                                   id_field='code',
                                                   profile='location')
        erecs, trecs = assemble_shelflist_test_records(test_itemdata,
                                                       id_field=id_field)
        return erecs, lrecs, trecs
    return _assemble_custom_shelflist


@pytest.fixture
def derive_updated_resource():
    """
    Pytest fixture. Returns a helper function that lets you provide a
    dict representing an existing resource (`old_item`), the relevant
    REST API serializer for that resource (`serializer`), and the
    SolrProfile object for that resource (`solr_profile`). Returns an
    updated version of that resource (dict), giving all fields listed
    in `which_fields` updated values. (Updates all fields by default.)
    The dict that's returned can be converted to JSON and submitted
    directly as a PUT request to update the resource via the API.
    """
    def _get_new_val(old_val, field_type):
        if field_type == 'str':
            return text_type('{} TEST').format((old_val or ''))
        if field_type == 'int':
            return (old_val or 0) + 1
        if field_type == 'bool':
            return not bool(old_val)
        if field_type == 'datetime':
            return '9999-01-01T00:00:00Z'
        return None

    def _derive_updated_resource(old_item, serializer, solr_profile,
                                 which_fields=None):
        new_item = {}
        for fname, fopts in serializer.fields.items():
            rendered_fname = serializer.render_field_name(fname)
            old_val = old_item[rendered_fname]
            solr_fname = fopts.get('source', fname)
            if (which_fields is None) or (rendered_fname in which_fields):
                field = solr_profile.fields.get(solr_fname, {})
                if field.get('multi', False):
                    old_val = old_val or [None]
                    new_val = [_get_new_val(o, fopts['type']) for o in old_val]
                else:
                    new_val = _get_new_val(old_val, fopts['type'])
            else:
                new_val = old_val
            new_item[rendered_fname] = new_val
        return new_item
    return _derive_updated_resource


@pytest.fixture
def filter_serializer_fields_by_opt():
    """
    Pytest fixture. Returns a helper function that lets you filter a
    list of REST API serializer fields based on the `serializer.fields`
    field options. Provide the `serializer`, the field opts `attr` and
    field opts attr `value`, and get a list of matching fields.
    """
    def _filter_serializer_fields_by_opt(serializer, attr, value):
        fields = []
        for fname, fopts in serializer.fields.items():
            rendered_fname = serializer.render_field_name(fname)
            if fopts.get(attr, None) == value:
                fields.append(rendered_fname)
        return fields
    return _filter_serializer_fields_by_opt


@pytest.fixture
def send_api_data(apiuser_with_custom_defaults, simple_sig_auth_credentials):
    """
    Pytest fixture. Returns a helper function that sends API data via
    the provided `api_client`, to the provided `url`, with the given
    `req_body`, via the given HTTP `method`. An API user is created and
    used for authentication. You may optionally supply a `content_type`
    string; if not supplied, then JSON is assumed by default (or
    json-patch for patch requests). The response object is returned.
    """
    content_types = {
        'put': 'application/json',
        'patch': 'application/json-patch+json',
        'post': 'application/json'
    }

    def _send_api_data(api_client, url, req_body, method, content_type=None):
        test_cls = apiuser_with_custom_defaults()
        api_user = test_cls.objects.create_user('test', 'sec', password='pw',
                                                email='test@test.com',
                                                first_name='F', last_name='L')
        content_type = content_type or content_types[method]
        api_client.credentials(**simple_sig_auth_credentials(api_user,
                                                             req_body))
        do_send = getattr(api_client, method)
        resp = do_send(url, req_body, content_type=content_type)
        api_client.credentials()
        return resp
    return _send_api_data


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
    list_resp = api_client.get(list(urls.values())[0])
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
                             ('shelflistItems', 'locations',
                              'location', 'shelflist'),
                             ('locations', 'shelflistItems',
                              'shelflist', 'location'),
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
        url = list(urls.values())[0]
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
    test_url = '{}?orderBy={}'.format(list(sl_urls.values())[0], order_by)
    response = api_client.get(test_url)
    assert response.status_code == 400
    assert 'not a valid field for ordering' in response.data['detail']


def test_shelflistitem_row_order(api_settings, shelflist_solr_env,
                                 get_shelflist_urls, api_client, redis_obj,
                                 get_found_ids):
    """
    The `shelflistitems` list view should list items in the same order
    that the shelflist manifest for that location lists them. The
    `rowNumber` value should be an incremented integer, starting at 0.
    """
    recs = shelflist_solr_env.records['shelflistitem']
    loc = recs[0]['location_code']
    loc_recs = [r for r in recs if r['location_code'] == loc]
    index = ShelflistItemIndex()
    manifest = index.get_location_manifest(loc)
    redis_key = '{}:{}'.format(REDIS_SHELFLIST_PREFIX, loc)
    redis_obj(redis_key).set(manifest)

    url = get_shelflist_urls(shelflist_solr_env.records['shelflistitem'])[loc]
    response = api_client.get(url)
    total = response.data['totalCount']
    found_ids = get_found_ids('id', response)
    row_numbers = get_found_ids('row_number', response)
    assert found_ids == manifest
    assert row_numbers == [num for num in range(0, total)]


def test_shelflistitem_putpatch_requires_auth(api_settings,
                                              assemble_custom_shelflist,
                                              get_shelflist_urls, api_client):
    """
    Saving data (via put or patch) to a shelflistitem resource should
    fail without authentication. A 403 status code should be returned,
    and the item should NOT be updated.
    """
    test_lcode, test_id = '1test', 99999999
    _, _, trecs = assemble_custom_shelflist(test_lcode, [(test_id, {})])
    url = '{}{}'.format(get_shelflist_urls(trecs)[test_lcode], test_id)
    before = api_client.get(url)
    put_resp = api_client.put(url, {})
    patch_resp = api_client.patch(url, {})
    after = api_client.get(url)
    assert put_resp.status_code == 403
    assert patch_resp.status_code == 403
    assert before.data == after.data


@pytest.mark.django_db
@pytest.mark.parametrize('method', ['put', 'patch'])
def test_shelflistitem_update_err_nonwritable(method, api_settings,
                                              assemble_custom_shelflist,
                                              shelflist_solr_env,
                                              filter_serializer_fields_by_opt,
                                              derive_updated_resource,
                                              send_api_data,
                                              get_shelflist_urls, api_client):
    """
    Attempting to update nonwritable fields raises an error,
    '... is not a writable field'. The item should NOT be updated.
    """
    test_lcode, test_id = '1test', 99999999
    _, _, trecs = assemble_custom_shelflist(test_lcode, [(test_id, {})])
    url = '{}{}'.format(get_shelflist_urls(trecs)[test_lcode], test_id)
    before = api_client.get(url)
    serializer = before.renderer_context['view'].get_serializer()
    profile = shelflist_solr_env.profiles['shelflistitem']
    try_item = derive_updated_resource(before.data, serializer, profile)

    if method == 'put':
        req_body = ujson.dumps(try_item)
    elif method == 'patch':
        req_body = jsonpatch.make_patch(before.data, try_item)

    unwritable = filter_serializer_fields_by_opt(serializer, 'writable', False)
    resp = send_api_data(api_client, url, req_body, method)
    after = api_client.get(url)

    assert resp.status_code == 400
    assert before.data == after.data
    for fname in unwritable:
        msg = '{} is not a writable field'.format(fname)
        assert msg in resp.data['detail']


@pytest.mark.django_db
@pytest.mark.parametrize('method', ['put', 'patch'])
def test_shelflistitem_update_items(method, api_settings,
                                    assemble_custom_shelflist,
                                    shelflist_solr_env,
                                    filter_serializer_fields_by_opt,
                                    derive_updated_resource, send_api_data,
                                    get_shelflist_urls, api_client):
    """
    Updating writable fields on shelflistitems should update/save the
    resource: it should update the writable fields that were changed
    and keep all other fields exactly the same.
    """
    test_lcode, test_id = '1test', 99999999
    _, _, trecs = assemble_custom_shelflist(test_lcode, [(test_id, {})])
    url = '{}{}'.format(get_shelflist_urls(trecs)[test_lcode], test_id)
    before = api_client.get(url)
    serializer = before.renderer_context['view'].get_serializer()
    writable = filter_serializer_fields_by_opt(serializer, 'writable', True)
    unwritable = filter_serializer_fields_by_opt(serializer, 'writable', False)
    profile = shelflist_solr_env.profiles['shelflistitem']
    try_item = derive_updated_resource(before.data, serializer, profile,
                                       which_fields=writable)

    if method == 'put':
        req_body = ujson.dumps(try_item)
    elif method == 'patch':
        req_body = jsonpatch.make_patch(before.data, try_item)

    resp = send_api_data(api_client, url, req_body, method)
    after = api_client.get(url)

    assert resp.status_code == 200
    assert resp.data['links']['self']['href'].endswith(url)
    assert resp.data['links']['self']['id'] == test_id

    print((before.data))
    print(try_item)
    print((after.data))

    for fname in writable:
        assert after.data[fname] == try_item[fname]
        assert after.data[fname] != before.data[fname]

    for fname in unwritable:
        assert after.data[fname] == try_item[fname]
        assert after.data[fname] == before.data[fname]


@pytest.mark.django_db
@pytest.mark.parametrize('fname_solr, fname_api, start_val, expect_error', [
    ('barcode', 'barcode', '9876543210', True),
    ('shelf_status', 'shelfStatus', 'onShelf', False),
])
def test_shelflistitem_put_data_missing_fields(fname_solr, fname_api,
                                               start_val, expect_error,
                                               api_settings,
                                               assemble_custom_shelflist,
                                               shelflist_solr_env,
                                               filter_serializer_fields_by_opt,
                                               derive_updated_resource,
                                               send_api_data,
                                               get_shelflist_urls, api_client):
    """
    A PUT request should replace the item being updated with the item
    in the request body. In other words: if a field isn't provided in
    the PUT request body, the field is set to None/null. This results
    in an error if the field previously had data and is not writable.
    """
    test_lcode, test_id = '1test', 99999999
    test_data = [(test_id, {fname_solr: start_val})]
    _, _, trecs = assemble_custom_shelflist(test_lcode, test_data)
    url = '{}{}'.format(get_shelflist_urls(trecs)[test_lcode], test_id)
    before = api_client.get(url)
    serializer = before.renderer_context['view'].get_serializer()
    profile = shelflist_solr_env.profiles['shelflistitem']
    writable = filter_serializer_fields_by_opt(serializer, 'writable', True)
    try_item = derive_updated_resource(before.data, serializer, profile,
                                       which_fields=writable)
    del(try_item[fname_api])
    req_body = ujson.dumps(try_item)
    resp = send_api_data(api_client, url, req_body, 'put')
    after = api_client.get(url)

    if expect_error:
        assert resp.status_code == 400
        assert before.data == after.data
        msg = '{} is not a writable field'.format(fname_api)
        assert msg in resp.data['detail']
    else:
        assert resp.status_code == 200
        assert before.data[fname_api] == start_val
        assert after.data[fname_api] is None


@pytest.mark.parametrize('test_data, search, expected',
                         compile_params(PARAMETERS__FIRSTITEMPERLOCATION),
                         ids=compile_ids(PARAMETERS__FIRSTITEMPERLOCATION))
def test_shelflist_firstitemperlocation_list(test_data, search, expected,
                                             api_settings, redis_obj,
                                             assemble_custom_shelflist,
                                             api_client, get_found_ids,
                                             do_filter_search):
    """
    The `firstitemperlocation` resource is basically a custom filter
    for `items` that submits a facet-query to Solr asking for the first
    item at each location code that matches the provided call number
    (plus cn type) or barcode. (Used by the Inventory App when doing a
    call number or barcode lookup without providing a location.) The
    `api` app contains a basic implementation, but it lacks the
    `shelflist`-specific extensions needed to make the Inventory App
    functionality work--namely the rowNumber from the shelflistitem
    manifest. This tests to make sure the `shelflist` app version
    overrides the `api` version and includes the rowNumber field.
    """
    test_data_by_location = {}
    for test_id, _, rec in test_data:
        lcode = rec['location_code']
        recs = test_data_by_location.get(lcode, []) + [(test_id, rec)]
        test_data_by_location[lcode] = recs

    index = ShelflistItemIndex()
    for test_lcode, data in test_data_by_location.items():
        assemble_custom_shelflist(test_lcode, data, id_field='record_number')
        manifest = index.get_location_manifest(test_lcode)
        redis_key = '{}:{}'.format(REDIS_SHELFLIST_PREFIX, test_lcode)
        redis_obj(redis_key).set(manifest)

    resource_url = '{}firstitemperlocation/'.format(API_ROOT)
    rsp = do_filter_search(resource_url, search, api_client)
    rsp_items = rsp.data['_embedded']['items']

    if expected is None:
        for item in rsp_items:
            assert item['locationCode'] not in list(
                test_data_by_location.keys())
    else:
        for exp_id in expected:
            exp_row = [i[1] for i in test_data if i[0] == exp_id][0]
            item = [i for i in rsp_items if i['recordNumber'] == exp_id][0]
            exp_sli = '{}/shelflistitems/{}'.format(item['locationCode'],
                                                    item['id'])
            assert item['rowNumber'] == exp_row
            assert item['_links']['shelflistItem']['href'].endswith(exp_sli)
