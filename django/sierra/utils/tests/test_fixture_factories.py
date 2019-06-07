"""
Contains tests for utils.test_helpers.fixture_factories.

This might be bad form, but, our tests test the fixture_factories
module by way of testing the actual test fixtures that implement those
factories. It *is* circular, and a little indirect, but it means we
don't have to duplicate those fixtures here to test them.
"""

import pytest
import importlib

from base import models as bm
from utils.test_helpers import solr_test_profiles as tp


# FIXTURES AND TEST DATA
# 
# Fixtures from django/sierra/conftest:
#     model_instance
#     global_model_instance
#     solr_data_assembler
#     global_solr_data_assembler
#     global_solr_conn
#     solr_search
#     installed_test_class

pytestmark = pytest.mark.django_db

TEST_MODEL_CLASS = bm.FixfldTypeMyuser


class ExistingTestClass(object):
    """
    This is just a dummy test class for use in the tests of
    `installed_test_class`.
    """
    arg = None


@pytest.fixture(scope='module')
def test_profile_definitions(global_solr_conn):
    """
    Pytest fixture that returns definitions for Solr profiles, for
    generating test data via the assembler fixtures.
    """
    hs_conn = global_solr_conn('haystack')
    return {
        'location': {
            'conn': hs_conn,
            'user_fields': tp.CODE_FIELDS,
            'field_gens': tp.LOCATION_GENS
        },
        'item': {
            'conn': hs_conn,
            'user_fields': tp.ITEM_FIELDS,
            'field_gens': tp.ITEM_GENS
        }
    }


@pytest.fixture
def test_assembler(solr_data_assembler, test_profile_definitions):
    """
    Pytest fixture that returns a simplified function-level
    solr_data_assembler, for testing the solr_data_assembler fixture
    and fixture_factory classes it depends on.
    """
    return solr_data_assembler(tp.SOLR_TYPES, tp.GLOBAL_UNIQUE_FIELDS,
                               tp.GENS, test_profile_definitions)


@pytest.fixture(scope='module')
def global_test_assembler(global_solr_data_assembler,
                          test_profile_definitions):
    """
    Module-scoped pytest fixture that returns a Solr test data
    assembler, for testing.
    """
    return global_solr_data_assembler(tp.SOLR_TYPES, tp.GLOBAL_UNIQUE_FIELDS,
                                      tp.GENS, test_profile_definitions)


@pytest.fixture(scope='module')
def test_solr_env(global_test_assembler):
    """
    Pytest fixture that mimics a fuller Solr environment one might set
    up using a global assembler, to serve as base-line test data for
    a suite (module) of related tests.
    """
    assembler = global_test_assembler
    gens = assembler.gen_factory
    loc_recs = assembler.make('location', 10)
    item_recs = assembler.make('item', 100,
        location_code=gens.choice([r['code'] for r in loc_recs])
    )
    assembler.save_all()
    return assembler


# TESTS

@pytest.mark.parametrize('glob_count', [0, 1, 2, 3])
def test_model_instance_fixtures(glob_count, model_instance,
                                 global_model_instance):
    """
    Creating a model instance using `model_instance` during a test
    should create an instance that is deleted when the test finishes
    and is NOT present when the next test begins.

    However, model instances created using `global_model_instance`
    should persist throughout all tests in the module.
    """
    tmodel = TEST_MODEL_CLASS
    loc_code, loc_name = 'lt1', 'LOCAL_TEST'
    glob_code, glob_name = 'gt', 'GLOBAL_TEST'
    past_glob_instances_exist = [True]
    for count in range(0, glob_count):
        match_code = '{}{}'.format(glob_code, count)
        instance_exists = len(tmodel.objects.filter(code=match_code)) == 1
        past_glob_instances_exist.append(instance_exists)
    new_gcode = '{}{}'.format(glob_code, glob_count)
    glob_instance_exists = len(tmodel.objects.filter(code=new_gcode)) == 1
    new_glob_instance = global_model_instance(tmodel, code=new_gcode,
                                              name=glob_name)
    new_glob_instance_exists = len(tmodel.objects.filter(code=new_gcode)) == 1

    loc_instance_exists = len(tmodel.objects.filter(code=loc_code)) == 1
    new_loc_instance = model_instance(tmodel, code=loc_code, name=loc_name)
    new_loc_instance_exists = len(tmodel.objects.filter(code=loc_code)) == 1

    print tmodel.objects.all()

    assert not loc_instance_exists
    assert not glob_instance_exists
    assert new_loc_instance_exists
    assert new_glob_instance_exists
    assert all(past_glob_instances_exist)


@pytest.mark.parametrize('count', [0, 1])
def test_model_instance_caller_fixture(count, model_instance_caller):
    """
    Creating a model instance using `model_instance_caller` during a
    test should create the model object, but the object should not yet
    be saved to the database until the `save` method is called.
    """
    tmodel = TEST_MODEL_CLASS
    loc_code, loc_name = 'lt1', 'LOCAL_TEST'
    loc_instance_exists = len(tmodel.objects.filter(code=loc_code)) == 1
    new_loc_instance = model_instance_caller(tmodel, code=loc_code,
                                             name=loc_name)
    new_loc_instance_saved_no = len(tmodel.objects.filter(code=loc_code)) == 1
    tmodel._write_override = True
    new_loc_instance.save()
    tmodel._write_override = False
    new_loc_instance_saved_yes = len(tmodel.objects.filter(code=loc_code)) == 1
    assert not loc_instance_exists
    assert not new_loc_instance_saved_no
    assert new_loc_instance_saved_yes


@pytest.mark.parametrize('iter_count', [0, 1, 2, 3])
def test_solr_data_assembler_fixtures(iter_count, test_assembler,
                                      test_solr_env, solr_search,
                                      global_solr_conn):
    """
    Creating and saving Solr data during a test using a
    `solr_data_assembler` fixture should ensure that the data created
    during the test (and ONLY the data created during the test) is
    cleared out after the test completes. In this set of tests, the
    `test_assembler` fixture is based on the `solr_data_assembler`
    fixture.

    However, data created via the `global_solr_data_assembler` should
    persist throughout multiple tests. The `test_solr_env` in this set
    of tests creates data using a `global_solr_data_assembler` before
    the tests run.
    """
    conn = global_solr_conn('haystack')
    start_num_locrecs_in_solr = len(solr_search(conn, 'type:Location'))
    start_num_itemrecs_in_solr = len(solr_search(conn, 'type:Item'))

    gens = test_assembler.gen_factory
    env_loc_recs = test_solr_env.records['location']
    env_item_recs = test_solr_env.records['item']
    test_str = '__TEST{}'.format(iter_count)

    pre_loc_in_solr = solr_search(conn, 'type:Location AND code:{}'
                                  ''.format(test_str))
    pre_items_in_solr = solr_search(conn, 'type:Item AND parent_bib_title:{}'
                                    ''.format(test_str))
    new_loc = test_assembler.make('location', 1, env_loc_recs,
                                  code=gens.static(test_str))
    new_item = test_assembler.make('item', 10, env_item_recs,
                                   parent_bib_title=gens.static(test_str))
    test_assembler.save_all()
    post_loc_in_solr = solr_search(conn, 'type:Location AND code:{}'
                                   ''.format(test_str))
    post_items_in_solr = solr_search(conn, 'type:Item AND parent_bib_title:{}'
                                     ''.format(test_str))
    end_num_locrecs_in_solr = len(solr_search(conn, 'type:Location'))
    end_num_itemrecs_in_solr = len(solr_search(conn, 'type:Item'))

    assert start_num_locrecs_in_solr == 10
    assert start_num_itemrecs_in_solr == 100
    assert len(test_solr_env.records['location']) == 10
    assert len(test_solr_env.records['item']) == 100
    assert len(pre_loc_in_solr) == 0
    assert len(pre_items_in_solr) == 0
    assert len(post_loc_in_solr) == 1
    assert len(post_items_in_solr) == 10
    assert len(test_assembler.records['location']) == 1
    assert len(test_assembler.records['item']) == 10
    assert end_num_locrecs_in_solr == 11
    assert end_num_itemrecs_in_solr == 110


@pytest.mark.parametrize('count', [0, 1])
def test_installed_test_class_fixture__new(count, installed_test_class):
    """
    Registering a test class via `installed_test_class` during a test
    should add the specified class to the module at the specified path.
    If a class by that name did not already exist in that namespace,
    then the class should simply be removed after the test.
    """
    modpath = 'utils.tests.test_fixture_factories'
    classname = 'NewTestClass'
    testclass = type(classname, (object,), {'arg': count})

    mod = importlib.import_module(modpath)
    class_installed_before = hasattr(mod, classname)
    result_tuple = installed_test_class(testclass, modpath)
    mod = importlib.import_module(modpath)
    class_installed_after = hasattr(mod, classname)

    assert not class_installed_before
    assert class_installed_after
    assert result_tuple == (modpath, testclass, None)
    assert getattr(mod, classname).arg == count


@pytest.mark.parametrize('count', [0, 1])
def test_installed_test_class_fixture__override(count, installed_test_class):
    """
    Registering a test class via `installed_test_class` during a test
    should override an existing class at that path, if one exists.
    After the test, the original class should be restored.
    """
    modpath = 'utils.tests.test_fixture_factories'
    classname = 'ExistingTestClass'
    testclass = type(classname, (object,), {'arg': count})

    mod = importlib.import_module(modpath)
    before_class = getattr(mod, classname)

    result_tuple = installed_test_class(testclass, modpath)
    
    mod = importlib.import_module(modpath)
    after_class = getattr(mod, classname)

    assert after_class == testclass
    assert before_class != testclass
    assert result_tuple == (modpath, testclass, before_class)
    assert getattr(before_class, 'arg') == None
    assert getattr(after_class, 'arg') == count
