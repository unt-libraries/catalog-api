"""
Contains all shared pytest fixtures and hooks
"""

import pytest
import importlib
import redis
import pysolr
import pytz
import hashlib
import urllib
import random
from datetime import datetime
from collections import OrderedDict

from django.conf import settings
from django.contrib.auth.models import User
from django.db.models import ObjectDoesNotExist
from rest_framework import test as drftest

from utils.redisobjs import RedisObject
from utils.test_helpers import (fixture_factories as ff,
                                solr_test_profiles as tp)
from export import models as em
from base import models as bm
from api.models import APIUser


# HOOKS -- These control setup / teardown for all tests.

def pytest_runtest_teardown(item, nextitem):
    """
    Flush the Redis appdata store after every single test. We can get
    away with this for Redis because it's fast. (There is no
    discernable difference in test run time with and without this.)
    """
    conn = redis.StrictRedis(**settings.REDIS_CONNECTION)
    conn.flushdb()


# General utility fixtures

@pytest.fixture(scope='module')
def mytempdir(tmpdir_factory):
    """
    Module-level fixture for creating a temporary dir for testing
    purposes.
    """
    return tmpdir_factory.mktemp('data')


@pytest.fixture(scope='module')
def make_tmpfile(mytempdir):
    """
    Module-level factory fixture for creating a data file in a temp
    directory. Pass the `data` to write along with the `filename`;
    returns a full absolute path to the written file.
    """
    def make(data, filename):
        path = mytempdir.join(filename)
        with open(str(path), 'w') as fh:
            fh.write(data)
        return path
    return make


@pytest.fixture(scope='function')
def model_instance():
    """
    Function-level pytest fixture. Returns a factory object that tracks
    model instances as they're created and clears them out when the
    test completes. Instances are created via the manager's `create`
    method.
    """
    with ff.FactoryTracker(ff.TestInstanceFactory()) as make:
        yield make


@pytest.fixture(scope='module')
def global_model_instance(django_db_blocker):
    """
    Module-level pytest fixture. Returns a factory object that tracks
    model instances as they're created and clears them out when the
    module completes. This is the same as `model_instance`, except
    instances persist throughout all tests in a module.
    """
    with django_db_blocker.unblock():
        with ff.FactoryTracker(ff.TestInstanceFactory()) as make:
            yield make


@pytest.fixture(scope='function')
def model_instance_caller():
    """
    Function-level pytest fixture. Returns a factory object that tracks
    model instances as they're created and clears them out when the
    test completes. Instances are created using whatever callable obj
    is passed to the factory.
    """
    with ff.FactoryTracker(ff.TestInstanceCallerFactory()) as make:
        yield make


@pytest.fixture(scope='function')
def setattr_model_instance():
    """
    Function-level pytest fixture. Yields a function that you can use
    to temporarily set model instance attributes, intended to be used
    to change data on a "permanent" fixture (like mock Sierra records),
    just for the duration of a test. Use it like you would `setattr`:
    setattr_model_instance(instance, attr, new_val). The first time an
    attribute is changed, the original DB value is cached. When the
    test is over, all cached changes are reverted.
    """
    def _set_write_override(instance, val):
        try:
            instance._write_override = val
        except AttributeError:
            pass

    def _set_and_save(instance, attr, value):
        _set_write_override(instance, True)
        setattr(instance, attr, value)
        instance.save()
        instance.refresh_from_db()
        _set_write_override(instance, False)

    cache = OrderedDict()
    def _setattr_model_instance(instance, attr, value):
        meta = instance._meta
        cache_key = '{}.{}.{}.{}'.format(meta.app_label, meta.model_name,
                                         instance.pk, attr)
        if cache_key not in cache:
            cache[cache_key] = (instance, attr, getattr(instance, attr))
        _set_and_save(instance, attr, value)

    yield _setattr_model_instance
        
    while cache:
        key = cache.keys()[-1]
        instance, attr, old_value = cache.pop(key)
        _set_and_save(instance, attr, old_value)


@pytest.fixture(scope='function')
def installed_test_class():
    """
    Function-level pytest fixture for temporarily installing a test
    class onto a particular module. The class may be installed as a
    brand new attribute, or it may override an existing class. Any
    changes are reverted after the test runs.
    """
    with ff.FactoryTracker(ff.TestClassRegistry()) as make:
        yield make


# Solr-related fixtures

@pytest.fixture(scope='function')
def solr_conn():
    """
    Function-level pytest fixture. Returns a callable factory object
    for creating a pysolr connection, for interacting with a Solr core.
    The connection is completely cleared (all records within the core
    deleted) when the test completes.
    """
    with ff.FactoryTracker(ff.TestSolrConnectionFactory()) as make:
        yield make


@pytest.fixture(scope='function')
def solr_conns(solr_conn, settings):
    """
    Function-level pytest fixture. Returns a dict of all configured
    Solr connections using the `solr_conn` fixture, to help ensure Solr
    records are cleared when a test finishes or raises an error.
    """
    return {name: solr_conn(name) for name in settings.HAYSTACK_CONNECTIONS}


@pytest.fixture(scope='module')
def global_solr_conn():
    """
    Module-level pytest fixture. Returns a callable factory object for
    creating a pysolr connection, for interacting with a Solr core. The
    connection is completely cleared (all records within the core
    deleted) when the module completes. This is the same as
    `solr_conn`, except that the connection isn't cleared after each
    function.
    """
    with ff.FactoryTracker(ff.TestSolrConnectionFactory()) as make:
        yield make


@pytest.fixture(scope='function')
def solr_data_assembler():
    """
    Function-level pytest fixture. Returns a callable factory for
    creating SolrTestDataAssembler objects, which allow you to make and
    manage multiple sets of Solr test data records. It tracks all
    records added via the factory and then clears them out when the
    test completes.
    """
    with ff.FactoryTracker(ff.SolrTestDataAssemblerFactory()) as make:
        yield make


@pytest.fixture(scope='module')
def global_solr_data_assembler():
    """
    Module-level pytest fixture. Returns a callable factory for
    creating SolrTestDataAssembler objects, which allow you to make and
    manage multiple sets of Solr test data records. It tracks all
    records added via the factory and then clears them out when the
    module completes. This is the same as `solr_data_assembler`, except
    that the records created persist throughout all tests in a module.
    """
    with ff.FactoryTracker(ff.SolrTestDataAssemblerFactory()) as make:
        yield make


@pytest.fixture(scope='module')
def solr_profile_definitions(global_solr_conn):
    """
    Pytest fixture that returns definitions for Solr profiles, for
    generating test data via the *_solr_data_factory fixtures.
    """
    hs_conn = global_solr_conn('haystack')
    bib_conn = global_solr_conn('bibdata')
    marc_conn = global_solr_conn('marc')
    return {
        'location': {
            'conn': hs_conn,
            'user_fields': tp.CODE_FIELDS,
            'field_gens': tp.LOCATION_GENS
        },
        'itype': {
            'conn': hs_conn,
            'user_fields': tp.CODE_FIELDS,
            'field_gens': tp.ITYPE_GENS
        },
        'itemstatus': {
            'conn': hs_conn,
            'user_fields': tp.CODE_FIELDS,
            'field_gens': tp.ITEMSTATUS_GENS
        },
        'item': {
            'conn': hs_conn,
            'user_fields': tp.ITEM_FIELDS,
            'field_gens': tp.ITEM_GENS
        },
        'eresource': {
            'conn': hs_conn,
            'user_fields': tp.ERES_FIELDS,
            'field_gens': tp.ERES_GENS
        },
        'bib': {
            'conn': bib_conn,
            'user_fields': tp.BIB_FIELDS,
            'field_gens': tp.BIB_GENS
        },
        'marc': {
            'conn': marc_conn,
            'user_fields': tp.MARC_FIELDS,
            'field_gens': tp.MARC_GENS
        }
    }


@pytest.fixture(scope='function')
def basic_solr_assembler(solr_data_assembler, solr_profile_definitions):
    """
    Function-scoped pytest fixture that returns a Solr test data
    assembler. Records created via this fixture within a test function
    are deleted when the test function finishes. (For more info about
    using Solr data assemblers, see the SolrTestDataAssemblerFactory
    class in utils.test_helpers.fixture_factories.)
    """
    return solr_data_assembler(tp.SOLR_TYPES, tp.GLOBAL_UNIQUE_FIELDS,
                               tp.GENS, solr_profile_definitions)


@pytest.fixture(scope='module')
def global_basic_solr_assembler(global_solr_data_assembler,
                                solr_profile_definitions):
    """
    Module-scoped pytest fixture that returns a Solr test data
    assembler. Records created via this fixture persist while all tests
    in the module run. (For more info about using Solr data assemblers,
    see the SolrTestDataAssemblerFactory class in
    utils.test_helpers.fixture_factories.)
    """
    return global_solr_data_assembler(tp.SOLR_TYPES, tp.GLOBAL_UNIQUE_FIELDS,
                                      tp.GENS, solr_profile_definitions)


@pytest.fixture
def solr_search():
    """
    Pytest fixture. Returns a search utility function that queries Solr
    via the given connection using the given query and parameters. It
    fetches all search results from the index and returns them.
    """
    def _solr_search(conn, query, **params):
        results, page, params['start'], params['rows'] = [], True, 0, 100
        while page:
            page = [item for item in conn.search(query, **params)]
            params['start'] += params['rows']
            results.extend(page)
        return results
    return _solr_search


# Redis-related fixtures

@pytest.fixture
def redis_obj():
    """
    Pytest fixture. Wraps utils.RedisObject.
    """
    class RedisObjectWrapper(object):
        def __init__(self):
            self.conn = RedisObject.conn

        def __call__(self, key):
            return RedisObject(*key.split(':'))

    return RedisObjectWrapper()


# Base app-related fixtures

@pytest.fixture
def sierra_records_by_recnum_range():
    """
    Return Sierra records by record number range.

    This is a pytest fixture that returns a set of objects based on the
    provided start and (optional) end record number. Uses the
    appropriate model based on the type of record, derived from the
    first character of the record numbers (e.g., b is BibRecord).

    Metadata about the filter used is added to an `info` attribute on
    the object queryset that is returned.
    """
    def _sierra_records_by_recnum_range(start, end=None, rm_only=False):
        rectype = start[0]
        filter_options = {'record_range_from': start, 
                          'record_range_to': end or start}
        if rm_only:
            model = bm.RecordMetadata
        else:
            modelname = bm.RecordMetadata.record_type_models[rectype]
            model = getattr(bm, modelname)
        recset = model.objects.filter_by('record_range', filter_options)
        recset.info = {
            'filter_method': 'filter_by',
            'filter_args': ['record_range', filter_options],
            'filter_kwargs': {},
            'export_filter_type': 'record_range',
            'export_filter_options': filter_options,
        }
        return recset
    return _sierra_records_by_recnum_range


@pytest.fixture
def sierra_full_object_set():
    """
    Return a full set of objects from a Sierra (base) model.

    This is a pytest fixture that returns a full set of model objects
    based on the type of model provided.

    Metadata about the filter used is added to an `info` attribute on
    the object queryset that is returned.
    """
    def _sierra_full_object_set(model_name):
        recset = getattr(bm, model_name).objects.all()
        recset.info = {
            'filter_method': 'all',
            'filter_args': [],
            'filter_kwargs': {},
            'export_filter_type': 'full_export',
            'export_filter_options': {}
        }
        return recset
    return _sierra_full_object_set


# Export app-related fixtures

@pytest.fixture
def record_sets(sierra_records_by_recnum_range, sierra_full_object_set):
    return {
        'bib_set': sierra_records_by_recnum_range('b4371446', 'b4517240'),
        'er_bib_set': sierra_records_by_recnum_range('b5784429', 'b5784819'),
        'eres_set': sierra_records_by_recnum_range('e1001249'),
        'item_set': sierra_records_by_recnum_range('i4264281', 'i4278316'),
        'itype_set': sierra_full_object_set('ItypeProperty'),
        'istatus_set': sierra_full_object_set('ItemStatusProperty'),
        'location_set': sierra_full_object_set('Location'),
        'bib_del_set': sierra_records_by_recnum_range('b1000001', 'b1000010',
                                                      rm_only=True),
        'item_del_set': sierra_records_by_recnum_range('i100631', 'i101010',
                                                       rm_only=True),
        'eres_del_set': sierra_records_by_recnum_range('e1000013', 'e1000179',
                                                       rm_only=True),
    }

@pytest.fixture
def export_type():
    def _export_type(code):
        return em.ExportType.objects.get(code=code)
    return _export_type


@pytest.fixture
def export_filter():
    def _export_filter(code):
        return em.ExportFilter.objects.get(code=code)
    return _export_filter


@pytest.fixture
def status():
    def _status(code):
        return em.Status.objects.get(code=code)
    return _status


@pytest.fixture
def new_export_instance(export_type, export_filter, status,
                        model_instance):
    def _new_export_instance(et_code, ef_code, st_code):
        try:
            test_user = User.objects.get(username='test')
        except User.DoesNotExist:
            test_user = model_instance(User, 'test', 'test@test.com',
                                            'testpass')
        return model_instance(
            em.ExportInstance,
            user=test_user,
            status=status(st_code),
            export_type=export_type(et_code),
            export_filter=export_filter(ef_code),
            errors=0,
            warnings=0,
            timestamp=datetime.now(pytz.utc)
        )
    return _new_export_instance


@pytest.fixture
def derive_exporter_class(installed_test_class, model_instance, export_type):
    """
    Pytest fixture.
    """
    def _install_exporter_class(newclass, modpath):
        installed_test_class(newclass, modpath)
        new_exptype_name = newclass.__name__
        classpath = '{}.{}'.format(modpath, new_exptype_name)
        try:
            model_name = newclass.model._meta.object_name
        except AttributeError:
            model_name = None
        new_exptype_info = { 'code': new_exptype_name, 'path': classpath,
                             'label': 'Do {} load'.format(new_exptype_name),
                             'description': new_exptype_name, 'order': 999 }
        models = importlib.import_module('export.models')
        new_exptype = model_instance(models.ExportType, **new_exptype_info)
        return newclass

    def _get_export_type(name):
        try:
            return export_type(name)
        except ObjectDoesNotExist:
            return None

    def _determine_mod_and_path(classname, modpath, exptype):
        mod = None if not modpath else importlib.import_module(modpath)
        if hasattr(mod, classname):
            return mod, modpath

        try:
            modpath = '.'.join(exptype.path.split('.')[0:-1])
        except TypeError, ObjectDoesNotExist:
            msg = ('In fixture `derive_exporter_class`, the supplied '
                   'base class name "{}" could not be resolved. It matches '
                   'neither any attribute of the supplied modpath "{}" nor '
                   'any ExportType.'.format(classname, modpath))
            raise pytest.UsageError(msg)
        return importlib.import_module(modpath), modpath

    def _derive_exporter_class(basename, local_modpath=None, newname=None,
                               attrs=None):
        exptype = _get_export_type(basename)
        mod, mpath = _determine_mod_and_path(basename, local_modpath, exptype)
        expclass_base = getattr(mod, basename)

        attrs = attrs or {}
        if 'index_config' not in attrs:
            for entry in getattr(expclass_base, 'index_config', []):
                conf = attrs.get('index_config', [])
                indclassname = '_{}'.format(entry.indexclass.__name__)
                indclass = type(indclassname, (entry.indexclass,), {})
                conf.append(type(entry)(entry.name, indclass, entry.conn))
                attrs['index_config'] = conf

        if 'children_config' not in attrs:
            for entry in getattr(expclass_base, 'children_config', []):
                childclass = _derive_exporter_class(entry.name, local_modpath)
                conf = attrs.get('children_config', [])
                conf.append(type(entry)(entry.name, childclass.__name__))
                attrs['children_config'] = conf

        name = str(newname or '_{}'.format(basename))
        newclass = type(name, (expclass_base,), attrs)
        return _install_exporter_class(newclass, mpath)
    return _derive_exporter_class


@pytest.fixture
def derive_compound_exporter_class(derive_exporter_class):
    def _derive_compound_exporter_class(p_basecls_name, p_path, p_attrs=None,
                                        children=None):
        parent = derive_exporter_class(p_basecls_name, p_path,
                                       newname='Parent',
                                       attrs=p_attrs or {})
        parent.children_config = tuple(
            parent.Child(c.__name__) for c in (children or [])
        )
        return parent
    return _derive_compound_exporter_class


@pytest.fixture
def derive_child_exporter_class(derive_exporter_class):
    def _derive_child_exporter_class(c_cls_name='Exporter',
                                     c_path='export.exporter', c_attrs=None,
                                     newname=None):
        return derive_exporter_class(c_cls_name, c_path,
                                     newname=newname or 'C',
                                     attrs=c_attrs or {})
    return _derive_child_exporter_class


@pytest.fixture
def new_exporter(new_export_instance):
    def _new_exporter(expclass, ef_code, st_code, options={}):
        et_code = expclass.__name__
        instance = new_export_instance(et_code, ef_code, st_code)
        assert expclass == instance.export_type.get_exporter_class()
        return expclass(instance.pk, ef_code, et_code, options)
    return _new_exporter


@pytest.fixture
def get_records_from_index(solr_conns, solr_search):
    """
    Pytest fixture that returns a test helper function for getting
    records from the provided `record_set` from the provided `index`
    object.
    """
    def _get_records_from_index(index, record_set, results=None):
        id_fname = index.reserved_fields['haystack_id']
        meta = index.get_model()._meta
        if results is None:
            conn = solr_conns[getattr(index, 'using', 'default')]
            results = solr_search(conn, '*')

        found_records = {}
        for record in record_set:
            cmp_id = '{}.{}.{}'.format(meta.app_label, meta.model_name,
                                       record.pk)
            matches = [r for r in results if r[id_fname] == cmp_id]
            if len(matches) == 1:
                found_records[record.pk] = matches[0]
        return found_records
    return _get_records_from_index


@pytest.fixture
def assert_records_are_indexed(get_records_from_index):
    """
    Pytest fixture that returns a test helper function for checking
    that the provided `record_set` has been indexed by the provided
    `index` object appropriately.
    """
    def _assert_records_are_indexed(index, record_set, results=None):
        results = get_records_from_index(index, record_set, results)
        for record in record_set:
            assert record.pk in results
            result = results[record.pk]
            for field in result.keys():
                schema_field = index.get_schema_field(field)
                assert schema_field is not None
                assert schema_field['stored']
    return _assert_records_are_indexed


@pytest.fixture
def assert_records_are_not_indexed(get_records_from_index):
    """
    Pytest fixture that returns a test helper function for checking
    that records in the provided `record_set` are NOT indexed in the
    Solr index represented by the provided `index` object.
    """
    def _assert_records_are_not_indexed(index, record_set, results=None):
        assert not get_records_from_index(index, record_set, results)
    return _assert_records_are_not_indexed


@pytest.fixture
def assert_all_exported_records_are_indexed(assert_records_are_indexed):
    """
    Pytest fixture that returns a test helper function for checking
    that the provided `record_set` has been indexed correctly as a
    result of the provided `exporter` class running its
    `export_records` method.
    """
    def _assert_all_exported_records_are_indexed(exporter, record_set):
        # Check results in all indexes for the parent test_exporter.
        test_indexes = getattr(exporter, 'indexes', {}).values()
        for index in test_indexes:
            assert_records_are_indexed(index, record_set)
        # Check results in all child indexes (if any).
        if hasattr(exporter, 'main_child'):
            child_rsets = exporter.derive_recordsets_from_parent(record_set)
            for child_name, child in exporter.children.items():
                child_indexes = getattr(child, 'indexes', {}).values()
                for index in child_indexes:
                    assert_records_are_indexed(index, child_rsets[child_name])
    return _assert_all_exported_records_are_indexed


@pytest.fixture
def assert_deleted_records_are_not_indexed(assert_records_are_not_indexed):
    """
    Pytest fixture that returns a test helper function for checking
    that records in the provided `record_set` have been removed from
    the appropriate indexes as the result of the provided `exporter`
    class running its `delete_records` method.
    """
    def _assert_deleted_records_are_not_indexed(exporter, record_set):
        # If the parent exporter has indexes, check them.
        indexes = getattr(exporter, 'indexes', {}).values()
        # Otherwise, if the parent has a `main_child` that has indexes,
        # then those are the ones to check.
        if not indexes and hasattr(exporter, 'main_child'):
            indexes = getattr(exporter.main_child, 'indexes', {}).values()

        for index in indexes:
            assert_records_are_not_indexed(index, record_set)
    return _assert_deleted_records_are_not_indexed


# API App related fixtures

@pytest.fixture(scope='module')
def api_solr_env(global_basic_solr_assembler):
    """
    Pytest fixture that generates and populates Solr with some random
    background test data for API integration tests. Fixture is module-
    scoped, so test data is regenerated each time the test module runs,
    NOT between tests.
    """
    assembler = global_basic_solr_assembler
    gens = assembler.gen_factory
    loc_recs = assembler.make('location', 10)
    itype_recs = assembler.make('itype', 10)
    status_recs = assembler.make('itemstatus', 10)
    bib_recs = assembler.make('bib', 100)
    item_recs = assembler.make('item', 200,
        location_code=gens.choice([r['code'] for r in loc_recs]),
        item_type_code=gens.choice([r['code'] for r in itype_recs]),
        status_code=gens.choice([r['code'] for r in status_recs]),
        parent_bib_id=gens(tp.choose_and_link_to_parent_bib(bib_recs))
    )
    eres_recs = assembler.make('eresource', 25)
    assembler.save_all()
    return assembler


@pytest.fixture
def api_client():
    """
    Pytest fixture that returns a new rest_framework.test.APIClient
    object.
    """
    return drftest.APIClient()


@pytest.fixture
def simple_sig_auth_credentials():
    """
    Pytest fixture that generates auth headers for the given `api_user`
    instance and optional `request_body` string so that a request using
    the custom api.simpleauth.SimpleSignatureAuthentication mechanism
    authenticates.
    """
    def _simple_sig_auth_credentials(api_user, request_body=''):
        since_1970 = (datetime.now() - datetime(1970, 1, 1))
        timestamp = str(int(since_1970.total_seconds() * 1000))
        hasher = hashlib.sha256('{}{}{}{}'.format(api_user.user.username,
                                                  api_user.secret, timestamp,
                                                  request_body))
        signature = hasher.hexdigest()
        return {
            'HTTP_X_USERNAME': 'test',
            'HTTP_X_TIMESTAMP': timestamp,
            'HTTP_AUTHORIZATION': 'Basic {}'.format(signature)
        }
    return _simple_sig_auth_credentials


@pytest.fixture(scope='function')
def apiuser_with_custom_defaults():
    """
    Function-level pytest fixture; returns a function to use for
    updating the APIUser class with custom default permissions.
    Restores the original defaults after the test runs.
    """
    def _apiuser_with_custom_defaults(defaults=None):
        defaults = defaults or {'test_create': False, 'test_update': False,
                                'test_delete': False}
        APIUser.permission_defaults = defaults.copy()
        return APIUser

    old_defaults = APIUser.permission_defaults.copy()
    yield _apiuser_with_custom_defaults
    APIUser.permission_defaults = old_defaults


@pytest.fixture(scope='function')
def pick_reference_object_having_link():
    """
    Pytest fixture. Returns a helper function that picks an object (in
    JSON terms) from the list of `objects` -- e.g., from an API list
    view -- and randomly chooses one that has the given `link_field`
    populated.
    """
    def _pick_reference_object_having_link(objects, link_field):
        choices = [o for o in objects if o['_links'].get(link_field, None)]
        return random.choice(choices)
    return _pick_reference_object_having_link


@pytest.fixture(scope='function')
def assert_obj_fields_match_serializer():
    """
    Pytest fixture. Returns a helper function that asserts that the
    given `obj` conforms to the given `serializer` -- all fields on the
    serializer are represented on the object.
    """
    def _assert_obj_fields_match_serializer(obj, serializer):
        for field_name in serializer.fields:
            assert serializer.render_field_name(field_name) in obj
    return _assert_obj_fields_match_serializer


@pytest.fixture(scope='function')
def get_linked_view_and_objects():
    """
    Pytest fixture. Returns a helper function, where, given a `client`
    object fixture and `ref_obj` (i.e. reference object, from the API)
    -- grab objects from the given `link_field` and return them.
    Returns a tuple: (view_obj, linked_objs). The returned linked_objs
    is ALWAYS a list, even if the link references a single object.
    """
    def _get_linked_view_and_objects(client, ref_obj, link_field):
        linked_objs = []
        try:
            resp = client.get(ref_obj['_links'][link_field]['href'])
        except TypeError:
            for link in ref_obj['_links'][link_field]:
                resp = client.get(link['href'])
                linked_objs.append(resp.data)
        else:
            try:
                linked_objs = resp.data['_embedded'].values()[0]
            except KeyError:
                linked_objs = [resp.data]
        return resp.renderer_context['view'], linked_objs
    return _get_linked_view_and_objects


@pytest.fixture(scope='function')
def assemble_test_records(api_solr_env, basic_solr_assembler):
    """
    Pytest fixture. Returns a helper function that assembles & loads a
    set of test records (for one test) into an existing module-level
    Solr test-data environment.

    Defaults to using `api_solr_env` and `basic_solr_assembler`
    fixtures, but you can override these via the `env` and `assembler`
    kwargs.

    Required args include a `profile` string, a set of static
    `test_data` partial records, and the name of the unique `id_field`
    for each record (for test_data record uniqueness). Returns a tuple
    of default solr_env records and the new test records that were
    loaded from the provided test data. len(env_recs) + len(test_recs)
    should == the total number of Solr records for that profile.
    """
    def _assemble_test_records(profile, id_field, test_data, env=api_solr_env,
                               assembler=basic_solr_assembler):
        env_recs = env.records[profile]
        test_recs = assembler.load_static_test_data(profile, test_data,
                                                    id_field=id_field,
                                                    context=env_recs)
        return (env_recs, test_recs)
    return _assemble_test_records


@pytest.fixture(scope='function')
def do_filter_search():
    """
    Pytest fixture. Returns a test helper function that performs the
    given `search` (e.g. search query string) on the given API
    `resource` via the given `api_client` fixture. Returns the
    response.
    """
    def _do_filter_search(resource, search, client):
        q = '&'.join(['='.join([urllib.quote_plus(v) for v in pair.split('=')])
                      for pair in search.split('&')])
        return client.get('{}/?{}'.format(resource, q))
    return _do_filter_search


@pytest.fixture(scope='function')
def get_found_ids():
    """
    Returns a list of values for identifying test records, in order,
    from the given `response` object. (Usually the response will come
    from calling `do_filter_search`.) `solr_id_field` is the name of
    that ID field as it exists in Solr.
    """
    def _get_found_ids(solr_id_field, response):
        serializer = response.renderer_context['view'].get_serializer()
        api_id_field = serializer.render_field_name(solr_id_field)
        total_found = response.data['totalCount']
        data = response.data.get('_embedded', {'data': []}).values()[0]
        # reality check: FAIL if there's any data returned on a different
        # page of results. If we don't return ALL available data, further
        # assertions will be invalid.
        assert len(data) == total_found
        return [r[api_id_field] for r in data]
    return _get_found_ids
