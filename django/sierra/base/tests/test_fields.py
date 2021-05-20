"""
Tests for custom fields.
"""

from __future__ import absolute_import
import re
import random
import pytest

from django.db import connections, models, IntegrityError
from django.core import serializers

from base import fields
from .vcftestmodels import models as vtm
import six


# FIXTURES AND TEST DATA

pytestmark = pytest.mark.django_db
TEST_MODEL_NAMES = ['VCFNameNumber', 'VCFNumberName', 'VCFParentInt',
                    'VCFParentStr', 'VCFParentIntID', 'VCFNonPK',
                    'VCFHyphenSep']


def build_app_models_environment():
    """
    Build a full test model environment for vcftestmodels.

    Uses vcftestmodels.models.get_app_models_environment, which returns
    an empty base.tests.helpers.AppModelsEnvironment object. Model
    classes are created here but must by initialized via the object's
    `migrate` method before they can be used.
    """
    app_models = vtm.get_app_models_environment()
    parent_int = app_models.make('ParentInt', {
        'id': models.IntegerField(primary_key=True),
        'name': models.CharField(max_length=255)
    })
    parent_str = app_models.make('ParentStr', {
        'id': models.CharField(max_length=255, primary_key=True),
        'name': models.CharField(max_length=255)
    })
    vcf_model = app_models.make('VCFModel', {
        'name': models.CharField(max_length=255, blank=True, null=True),
        'number': models.IntegerField(null=True),
        'parent_int': models.ForeignKey(parent_int, null=True,
                                        db_constraint=False,
                                        on_delete=models.DO_NOTHING),
        'parent_str': models.ForeignKey(parent_str, null=True,
                                        db_constraint=False,
                                        on_delete=models.DO_NOTHING),
    }, meta_options={'abstract': True})
    app_models.make('VCFNameNumber', {
        'vcf': fields.VirtualCompField(
            primary_key=True,
            partfield_names=['name', 'number'])
    }, modeltype=vcf_model)
    app_models.make('VCFNumberName', {
        'vcf': fields.VirtualCompField(
            primary_key=True,
            partfield_names=['number', 'name'])
    }, modeltype=vcf_model)
    app_models.make('VCFParentInt', {
        'vcf': fields.VirtualCompField(
            primary_key=True,
            partfield_names=['name', 'number', 'parent_int'])
    }, modeltype=vcf_model)
    app_models.make('VCFParentStr', {
        'vcf': fields.VirtualCompField(
            primary_key=True,
            partfield_names=['name', 'number', 'parent_str'])
    }, modeltype=vcf_model)
    app_models.make('VCFParentIntID', {
        'vcf': fields.VirtualCompField(
            primary_key=True,
            partfield_names=['name', 'number', 'parent_int_id'])
    }, modeltype=vcf_model)
    app_models.make('VCFNonPK', {
        'vcf': fields.VirtualCompField(partfield_names=['name', 'number'])
    }, modeltype=vcf_model)
    app_models.make('VCFHyphenSep', {
        'vcf': fields.VirtualCompField(
            primary_key=True,
            partfield_names=['name', 'number'],
            separator='-')
    }, modeltype=vcf_model)
    return app_models


@pytest.fixture(scope='module')
def app_models_env(django_db_blocker):
    """
    Pytest fixture. Builds the app_models obj using `make_app_models`
    and calls the `migrate` method to set up the models. Returns the
    app_models object. Teardown is handled by app_models, upon exit of
    its `with` block, after tests in this module have completed.
    """
    with django_db_blocker.unblock():
        with build_app_models_environment() as app_models:
            app_models.migrate()
            yield app_models


@pytest.fixture
def testmodels(app_models_env):
    """
    Pytest fixture that deletes existing records in any models in
    app_models_env.models (an OrderedDict used to access the models).
    Returns the OrderedDict, with all models emptied.
    """
    testmodels = app_models_env.models
    for model in testmodels.values():
        if hasattr(model, 'objects') and len(model.objects.all()):
            model.objects.all().delete()
    return testmodels


@pytest.fixture
def make_instance(testmodels):
    """
    Pytest fixture that returns a factory function that creates (and
    returns) a test model instance, given the `fields` kwargs.

    The `parent_int` and `parent_str` fields are converted to the
    appropriate model instances.
    """
    def _make_instance(modelname, name, number, parent_int, parent_str):
        fields = { 'name': name, 'number': number, 'parent_int': parent_int,
                   'parent_str': parent_str }
        testmodel = testmodels[modelname]
        pint_def = (testmodels['ParentInt'], 'parent_int')
        pstr_def = (testmodels['ParentStr'], 'parent_str')
        for (pmodel, pfield) in (pint_def, pstr_def):
            pid = fields.get(pfield, None)
            if pid is not None:
                try:
                    fields[pfield] = pmodel.objects.get(pk=pid)
                except pmodel.DoesNotExist:
                    pname = 'parent{}'.format(pid)
                    fields[pfield] = pmodel.objects.create(id=pid,
                                                           name=pname)
        return testmodel.objects.create(**fields)
    return _make_instance


@pytest.fixture
def noise_data():
    """
    Pytest fixture that returns a generator for creating `n` sets of
    parameters that can be passed to `make_instance` to create noise
    data. 
    """
    def _noise_data(n):
        count = 1
        while count <= n:
            parent_int = random.randint(0,11) or None
            parent_str = six.text_type(random.randint(0,11) or None)
            yield ('noise{}'.format(count), count, parent_int, parent_str)
            count += 1
    return _noise_data


@pytest.fixture
def get_db_columns():
    """
    Pytest fixture that returns a list of column names from the DB,
    based on the given model.
    """
    def _get_db_columns(model):
        with connections['default'].cursor() as cursor:
            sql = "SHOW COLUMNS FROM {}".format(model._meta.db_table)
            cursor.execute(sql)
            rows = [r for r in cursor.fetchall()]
        return [r[0] for r in rows]
    return _get_db_columns

@pytest.fixture
def do_for_multiple_models():
    """
    Pytest fixture that performs a manager/queryset action across
    multiple models and returns the results, if any. 
    """
    def _do_for_multiple_models(models, action):
        results = []
        for m in models:
            if action == 'delete':
                m.objects.all().delete()
            else:
                results.extend(getattr(m.objects, action)())
        return results
    return _do_for_multiple_models


# TESTS

@pytest.mark.parametrize('modelname', TEST_MODEL_NAMES)
def test_vcfield_exists_but_is_virtual(modelname, testmodels,
                                       get_db_columns):
    """
    VirtualCompField objects should never have a DB column behind them.
    """
    testmodel = testmodels[modelname]
    testfield = testmodel._meta.get_field('vcf')
    assert not testfield.concrete
    assert not testfield.column
    assert testfield.name not in get_db_columns(testmodel)


def test_vcfield_relation_or_fkid_use_same_partfields(testmodels):
    """
    Using a ForeignKey field in a VirtualCompField's definition results
    in the same `partfields` as using the table column name for the FK
    field. In other words: given a model where you have an FK relation
    to another model via the model field `parent`, where the FK ID
    value in the table column is `parent_id` -- and you want to use
    that FK relation (the ID) as part of the composite field value.
    When instantiating the VirtualCompField, you can use either the name
    `parent` or the name `parent_id` in the `partfield_names` kwarg,
    so long as both are accessors for the same model field.
    """
    rel_vcf = testmodels['VCFParentInt']._meta.get_field('vcf')
    fkid_vcf = testmodels['VCFParentIntID']._meta.get_field('vcf')
    assert rel_vcf.partfields == fkid_vcf.partfields


@pytest.mark.parametrize('modelname, name, number, parent_int, parent_str, '
                         'expected_vcf', [
    ('VCFNameNumber', 'aaa', 123, 1, '1', ('aaa', 123)),
    ('VCFNameNumber', 'aaa', 0, 1, '1', ('aaa', 0)),
    ('VCFNameNumber', 'aaa', None, 1, '1', ('aaa', None)),
    ('VCFNameNumber', None, 123, 1, '1', (None, 123)),
    ('VCFNameNumber', '', 123, 1, '1', ('', 123)),
    ('VCFNumberName', 'aaa', 123, 1, '1', (123, 'aaa')),
    ('VCFNumberName', None, None, 1, '1', (None, None)),
    ('VCFNonPK', 'aaa', 123, 1, '1', ('aaa', 123)),
    ('VCFHyphenSep', 'aaa', 123, 1, '1', ('aaa', 123)),
    ('VCFParentInt', 'aaa', 123, 1, '2', ('aaa', 123, 1)),
    ('VCFParentInt', 'aaa', 123, None, '2', ('aaa', 123, None)),
    ('VCFParentInt', None, 123, None, '2', (None, 123, None)),
    ('VCFParentInt', 'aaa', None, None, '2', ('aaa', None, None)),
    ('VCFParentStr', 'aaa', 123, 1, '2', ('aaa', 123, '2')),
    ('VCFParentStr', 'aaa', 123, 1, None, ('aaa', 123, None)),
    ('VCFParentIntID', 'aaa', 123, 1, '2', ('aaa', 123, 1)),
    ('VCFParentIntID', 'aaa', 123, None, '2', ('aaa', 123, None)),
])
def test_vcfield_value_access(modelname, name, number, parent_int,
                              parent_str, expected_vcf, make_instance):
    """
    Accessing a VirtualCompField on a model instance should return a
    list with the correct components in the correct order.
    """
    test_inst = make_instance(modelname, name, number, parent_int, parent_str)
    assert test_inst.vcf == expected_vcf


def test_vcfield_broken_fk_value_access(testmodels, make_instance):
    """
    In the rare case that an FK partfield value refers to a
    non-existent related object, the value on DB column in the table
    for the reference object (which DOES exist) should be used if/when
    the VirtualCompField value is accessed.
    """
    test_inst = make_instance('VCFParentInt', 'aaa', 123, 99, None)
    # Deleting the ParentInt instance referenced on test_inst leaves a
    # broken FK reference on test_inst because the FK definition uses
    # `on_delete=models.DO_NOTHING`.
    testmodels['ParentInt'].objects.get(pk=99).delete()
    qset = testmodels['VCFParentInt'].objects.all()
    assert test_inst.vcf == ('aaa', 123, 99)
    assert len(qset) == 1
    assert qset[0].vcf == test_inst.vcf


def test_vcfield_cannot_set_value(make_instance):
    """
    Attempting to set a value on a VirtualCompField explicitly should
    raise a NotImplementedError.
    """
    test_inst = make_instance('VCFNameNumber', 'aaa', 123, 1, '2')
    with pytest.raises(NotImplementedError):
        test_inst.vcf = ('should not work!', 123)


@pytest.mark.parametrize('modelname, name, number, parent_int, parent_str, '
                         'expected_pk', [
    ('VCFNameNumber', 'aaa', 123, 1, '1', ('aaa', 123)),
    ('VCFNameNumber', 'aaa', 0, 1, '1', ('aaa', 0)),
    ('VCFNameNumber', 'aaa', None, 1, '1', ('aaa', None)),
    ('VCFNameNumber', None, 123, 1, '1', (None, 123)),
    ('VCFNameNumber', '', 123, 1, '1', ('', 123)),
    ('VCFNumberName', 'aaa', 123, 1, '1', (123, 'aaa')),
    ('VCFNumberName', None, None, 1, '1', (None, None)),
    ('VCFParentInt', 'aaa', 123, 1, '2', ('aaa', 123, 1)),
    ('VCFParentInt', 'aaa', 123, None, '2', ('aaa', 123, None)),
    ('VCFParentInt', None, 123, None, '2', (None, 123, None)),
    ('VCFParentInt', 'aaa', None, None, '2', ('aaa', None, None)),
    ('VCFParentStr', 'aaa', 123, 1, '2', ('aaa', 123, '2')),
    ('VCFParentStr', 'aaa', 123, 1, None, ('aaa', 123, None)),
    ('VCFParentIntID', 'aaa', 123, 1, '2', ('aaa', 123, 1)),
    ('VCFParentIntID', 'aaa', 123, None, '2', ('aaa', 123, None)),
])
def test_vcfield_pk_access(modelname, name, number, parent_int, parent_str,
                           expected_pk, make_instance):
    """
    When a VirtualCompField is a PK on a model, accessing `instance.pk`
    should return the correct value.
    """
    test_inst = make_instance(modelname, name, number, parent_int, parent_str)
    assert test_inst.pk == expected_pk


@pytest.mark.parametrize('modelname, name, number, parent_int, parent_str, '
                         'expected_pk', [
    ('VCFNameNumber', 'aaa', 123, 1, '1', ('aaa', 123)),
    ('VCFNameNumber', 'aaa', 0, 1, '1', ('aaa', 0)),
    ('VCFNameNumber', 'aaa', None, 1, '1', ('aaa', None)),
    ('VCFNameNumber', None, 123, 1, '1', (None, 123)),
    ('VCFNameNumber', '', 123, 1, '1', ('', 123)),
    ('VCFNumberName', 'aaa', 123, 1, '1', (123, 'aaa')),
    ('VCFNumberName', None, None, 1, '1', (None, None)),
    ('VCFParentInt', 'aaa', 123, 1, '2', ('aaa', 123, 1)),
    ('VCFParentInt', 'aaa', 123, None, '2', ('aaa', 123, None)),
    ('VCFParentInt', None, 123, None, '2', (None, 123, None)),
    ('VCFParentInt', 'aaa', None, None, '2', ('aaa', None, None)),
    ('VCFParentStr', 'aaa', 123, 1, '2', ('aaa', 123, '2')),
    ('VCFParentStr', 'aaa', 123, 1, None, ('aaa', 123, None)),
    ('VCFParentIntID', 'aaa', 123, 1, '2', ('aaa', 123, 1)),
    ('VCFParentIntID', 'aaa', 123, None, '2', ('aaa', 123, None)),
])
def test_vcfield_pk_lookups_work(modelname, name, number, parent_int,
                                 parent_str, expected_pk, testmodels,
                                 make_instance, noise_data):
    """
    When a VirtualCompField is a PK on a model, lookups that use `pk`
    as a field (instead of the field name) should work.
    """
    tmodel = testmodels[modelname]
    test_inst = make_instance(modelname, name, number, parent_int, parent_str)
    noise = [make_instance(modelname, *f) for f in noise_data(5)]
    assert len(tmodel.objects.all()) == 6
    assert tmodel.objects.get(pk=expected_pk) == test_inst
    assert tmodel.objects.filter(pk=expected_pk)[0] == test_inst


@pytest.mark.parametrize('modelname, name, number, parent_int, parent_str', [
    ('VCFNameNumber', 'aaa', 123, 1, '1'),
    ('VCFNameNumber', 'aaa', 0, 1, '1'),
    ('VCFNameNumber', '', 123, 1, '1'),
    ('VCFNumberName', 'aaa', 123, 1, '1'),
    ('VCFParentInt', 'aaa', 123, 1, '2'),
    ('VCFParentStr', 'aaa', 123, 1, '2'),
    ('VCFParentIntID', 'aaa', 123, 1, '2'),
    ('VCFHyphenSep', 'aaa', 123, 1, '1'),
])
def test_vcfield_pk_uniqueness_works(modelname, name, number, parent_int,
                                     parent_str, testmodels, make_instance,
                                     noise_data):
    """
    When a VirtualCompField is a PK on a model, uniqueness of the
    fields that compose the VCF should be enforced. (Note that this
    fails for NULL values because the `unique_together` constraint used
    fails for NULL values! Addressed via the next test.)
    """
    tmodel = testmodels[modelname]
    test_inst = make_instance(modelname, name, number, parent_int, parent_str)
    noise = [make_instance(modelname, *f) for f in noise_data(5)]
    assert len(tmodel.objects.all()) == 6
    with pytest.raises(IntegrityError):
        make_instance(modelname, name, number, parent_int, parent_str)


@pytest.mark.parametrize('modelname, name, number, parent_int, parent_str', [
    ('VCFNameNumber', 'aaa', None, 1, '1'),
    ('VCFNameNumber', None, 123, 1, '1'),
    ('VCFNumberName', None, None, 1, '1'),
    ('VCFParentInt', 'aaa', 123, None, '2'),
    ('VCFParentInt', None, 123, None, '2'),
    ('VCFParentInt', 'aaa', None, None, '2'),
    ('VCFParentStr', 'aaa', 123, 1, None),
    ('VCFParentIntID', 'aaa', 123, None, '2'),
])
def test_vcfield_uniqueness_fails(modelname, name, number, parent_int,
                                  parent_str, testmodels, make_instance,
                                  noise_data):
    """
    When a VirtualCompField is a PK on a model, enforcing uniqueness of
    the fields that compose the VCF is impossible and will fail if any
    of the fields is NULL.

    The purpose of this test is mainly to document this behavior and
    assert that this is a known edge case to be aware of. If a future
    Django update ever addresses this or we decide we need to try to
    fix it, this test will help.
    """
    tmodel = testmodels[modelname]
    test_inst = make_instance(modelname, name, number, parent_int, parent_str)
    noise = [make_instance(modelname, *f) for f in noise_data(5)]
    dupe = make_instance(modelname, name, number, parent_int, parent_str)
    assert len(tmodel.objects.all()) == 7
    assert test_inst and dupe and (test_inst.pk == dupe.pk)


@pytest.mark.parametrize('modelname, name, number, parent_int, parent_str', [
    ('VCFNonPK', 'aaa', 123, 1, '1'),
    ('VCFNonPK', None, 123, 1, '1'),
])
def test_vcfield_nonpk_not_unique(modelname, name, number, parent_int,
                                  parent_str, testmodels, make_instance,
                                  noise_data):
    """
    When a VirtualCompField is NOT a PK on a model, there are no
    uniqueness constraints; duplicates are allowed.
    """
    tmodel = testmodels[modelname]
    test_inst = make_instance(modelname, name, number, parent_int, parent_str)
    noise = [make_instance(modelname, *f) for f in noise_data(5)]
    dupe = make_instance(modelname, name, number, parent_int, parent_str)
    assert len(tmodel.objects.all()) == 7
    assert test_inst and dupe and (test_inst.vcf == dupe.vcf)


@pytest.mark.parametrize('modelname, name, number, parent_int, parent_str, '
                         'lookup_arg', [
    ('VCFNameNumber', 'aaa', 123, 1, '1', ('aaa', 123)),
    ('VCFNameNumber', 'aaa', 123, 1, '1', ['aaa', '123']),
    ('VCFNameNumber', 'aaa', 123, 1, '1', 'aaa|_|123'),
    ('VCFNameNumber', 'aaa', 0, 1, '1', ['aaa', 0]),
    ('VCFNameNumber', 'aaa', None, 1, '1', ['aaa', None]),
    ('VCFNameNumber', 'aaa', None, 1, '1', 'aaa|_|'),
    ('VCFNameNumber', None, 123, 1, '1', [None, 123]),
    ('VCFNameNumber', None, 123, 1, '1', '|_|123'),
    ('VCFNameNumber', '', 123, 1, '1', ['', 123]),
    ('VCFNumberName', 'aaa', 123, 1, '1', [123, 'aaa']),
    ('VCFNumberName', None, None, 1, '1', [None, None]),
    ('VCFParentInt', 'aaa', 123, 1, '2', ['aaa', 123, 1]),
    ('VCFParentInt', 'aaa', 123, None, '2', ['aaa', 123, None]),
    ('VCFParentInt', None, 123, None, '2', [None, 123, None]),
    ('VCFParentInt', 'aaa', None, None, '2', ['aaa', None, None]),
    ('VCFParentStr', 'aaa', 123, 1, '2', ['aaa', 123, '2']),
    ('VCFParentStr', 'aaa', 123, 1, None, ['aaa', 123, None]),
    ('VCFParentIntID', 'aaa', 123, 1, '2', ['aaa', 123, '1']),
    ('VCFParentIntID', 'aaa', 123, None, '2', ['aaa', 123, None]),
    ('VCFNonPK', 'aaa', 123, 1, '1', ['aaa', 123]),
    ('VCFNonPK', None, 123, 1, '1', [None, 123]),
    ('VCFHyphenSep', 'aaa', 123, 1, '1', ['aaa', 123]),
    ('VCFHyphenSep', None, 123, 1, '1', [None, 123]),
])
def test_vcfield_exact_lookups_work(modelname, name, number, parent_int,
                                    parent_str, lookup_arg, testmodels,
                                    make_instance, noise_data):
    """
    Exact-match lookups on VirtualCompFields that match something
    should behave as follows:
    * The obj manager `get` method should return the correct instance.
    * The obj manager `filter` method should return a QuerySet
      containing the correct instance (and ONLY that instance).
    * The obj manager `exclude` method should return a QuerySet that
      doesn't include the test instance.
    * The SQL produced by such lookups should contain a WHERE clause
      with the appropriate partfield expressions ANDed together.
    """
    tmodel = testmodels[modelname]
    test_inst = make_instance(modelname, name, number, parent_int, parent_str)
    noise = [make_instance(modelname, *f) for f in noise_data(5)]
    qset = tmodel.objects.filter(vcf=lookup_arg)
    exclude_qset = tmodel.objects.exclude(vcf=lookup_arg)
    where = qset.query.sql_with_params()[0].split(' WHERE ')[1]
    vcf_cols = [pf.column for pf in tmodel._meta.get_field('vcf').partfields]
    partfield_pattern = r'\W.* AND .*\W'.join(vcf_cols)
    where_pattern = r'\W{}\W'.format(partfield_pattern)
    assert len(tmodel.objects.all()) == 6
    assert tmodel.objects.get(vcf=lookup_arg) == test_inst
    assert len(qset) == 1 and test_inst in qset
    assert test_inst not in exclude_qset
    assert re.search(where_pattern, where)


@pytest.mark.parametrize('modelname, name, number, parent_int, parent_str, '
                         'lookup_arg', [
    ('VCFParentInt', 'aaa', 123, 1, '1', ['no', 'no', 'no']),
    ('VCFParentInt', 'aaa', 123, 1, '1', 'no|_|no|_|no'),
    ('VCFParentInt', 'aaa', 123, 1, '1', ['aaa', 123]),
    ('VCFParentInt', 'aaa', 123, 1, '1', 'aaa|_|123'),
    ('VCFParentInt', 'aaa', 123, None, '1', ('aaa', 123)),
    ('VCFParentInt', 'aaa', 123, None, '1', ['aaa', 123, '']),
    ('VCFParentInt', 'aaa', 123, 1, '1', 123),
    ('VCFParentInt', 'aaa', 123, 1, '1', ''),
    ('VCFParentInt', 'aaa', 123, 1, '1', 'aaa'),
    ('VCFParentInt', 'aaa', 123, 1, '1', ['aaa', 'aaa', 1]),
    ('VCFParentInt', 'aaa', 123, 1, '1', {'what': 'even', 'is': 'this?'}),
])
def test_vcfield_exact_lookup_errors(modelname, name, number, parent_int,
                                     parent_str, lookup_arg, testmodels,
                                     make_instance):
    """
    Exact-match lookups on VirtualCompFields that have an ill-formed
    lookup arg should raise a ValueError.
    """
    tmodel = testmodels[modelname]
    test_inst = make_instance(modelname, name, number, parent_int, parent_str)
    with pytest.raises(ValueError):
        tmodel.objects.get(vcf=lookup_arg)
        tmodel.objects.filter(vcf=lookup_arg)


@pytest.mark.parametrize('modelname, name, number, parent_int, parent_str, '
                         'lookup_arg', [
    ('VCFNameNumber', '', 123, 1, '1', '|_|123'),
    ('VCFParentInt', 'aaa', 123, 1, '1', ['aaa', 123, 456]),
    ('VCFParentInt', 'aaa', 123, 1, '1', 'aaa|_|123|_|456'),
    ('VCFParentInt', 'aaa', 123, 1, '1', ('aaa', 0, 1)),
    ('VCFParentInt', 'aaa', 123, 1, '1', ['bbb', 123, 1]),
    ('VCFParentInt', 'aaa', 123, 1, '1', ['aaa', '12', 1]),
    ('VCFParentInt', 'aaa', 123, 1, '1', ['aaa', 123, '456']),
    ('VCFParentInt', 'aaa', 123, 1, '1', ['123', '123', '456']),
])
def test_vcfield_nonmatch_exact_lookups(modelname, name, number, parent_int,
                                        parent_str, lookup_arg, testmodels,
                                        make_instance):
    """
    Exact-match lookups on VirtualCompFields that match nothing but are
    well-formed should behave as follows:
    * The obj manager `get` method should raise a DoesNotExist error.
    * The obj manager `filter` method should return a blank QuerySet.
    """
    tmodel = testmodels[modelname]
    test_inst = make_instance(modelname, name, number, parent_int, parent_str)
    assert len(tmodel.objects.filter(vcf=lookup_arg)) == 0
    with pytest.raises(tmodel.DoesNotExist):
        tmodel.objects.get(vcf=lookup_arg)


@pytest.mark.parametrize('modelname, name, number, parent_int, parent_str, '
                         'lookup_type, lookup_arg', [
    ('VCFNameNumber', 'aaa', 123, 1, '1', 'icontains', 'a'),
    ('VCFNameNumber', 'aaa', 0, 1, '1', 'in', [('aaa', 0), ('aaa', 1)]),
    ('VCFNameNumber', 'aaa', 0, 1, '1', 'iexact', ['AAA', 0]),
    ('VCFNameNumber', 'aaa', None, 1, '1', 'startswith', ['aa']),
    ('VCFNameNumber', None, 123, 1, '1', 'endswith', [123]),
    ('VCFNameNumber', '', 123, 1, '1', 'contains', 2),
    ('VCFNumberName', 'aaa', 123, 1, '1', 'gte', [123]),
    ('VCFNumberName', 'aaa', 123, 1, '1', 'lte', [124, None]),
    ('VCFParentInt', 'aaa', 123, 1, '2', 'contains', [123]),
    ('VCFParentInt', 'aaa', 123, None, '2', 'range', (['aaa', 123],
                                                      ['aaa', 124])),
    ('VCFParentInt', None, 123, None, '2', 'isnull', False),
    ('VCFParentInt', 'aaa', None, None, '2', 'regex', '^aa'),
    ('VCFParentStr', 'aaa', 123, 1, '2', 'regex', '^aaa\|_\|12'),
])
def test_vcfield_other_lookups_work(modelname, name, number, parent_int,
                                    parent_str, lookup_type, lookup_arg,
                                    testmodels, make_instance, noise_data):
    """
    Lookups on VirtualCompFields other than exact-match should behave
    as follows:
    * The correct instance should be in the QuerySet the obj manager
      `filter` method returns.
    * The same instance should NOT appear in a QuerySet returned using
      the `exclude` obj manager method.
    * The SQL produced by such lookups should contain a WHERE clause
      with the appropriate partfield expressions ANDed together.

    Note that this test is far from comprehensive. The intention isn't
    to exhaustively test these lookups, but to test a decent enough
    variety so we're reasonably sure built-in lookups work as expected.
    """
    tmodel = testmodels[modelname]
    test_inst = make_instance(modelname, name, number, parent_int, parent_str)
    noise = [make_instance(modelname, *f) for f in noise_data(5)]
    lookup_str = 'vcf__{}'.format(lookup_type)
    qset = tmodel.objects.filter(**{lookup_str: lookup_arg})
    exclude_qset = tmodel.objects.exclude(**{lookup_str: lookup_arg})
    where = qset.query.sql_with_params()[0].split(' WHERE ')[1]
    vcf_cols = [pf.column for pf in tmodel._meta.get_field('vcf').partfields]
    partfield_pattern = r'\W.*\W'.join(vcf_cols)
    where_pattern = r'^CONCAT\(.*\W{}\W'.format(partfield_pattern)
    assert len(tmodel.objects.all()) == 6
    assert test_inst in [r for r in qset]
    assert test_inst not in [r for r in exclude_qset]
    assert re.search(where_pattern, where)


@pytest.mark.parametrize('modelname, data', [
    ('VCFNameNumber', [ ('aaa', 1, 1, '1'), ('aaa', 3, 1, '1'),
                        ('aaa', 2, 1, '1') ]),
    ('VCFNameNumber', [ ('aab', 1, 1, '1'), ('aaa', 1, 1, '1'),
                        ('aac', 1, 1, '1') ]),
    ('VCFNonPK', [ ('aaa', 1, 1, '1'), ('aaa', 3, 1, '1'),
                   ('aaa', 2, 1, '1') ]),
    ('VCFNonPK', [ ('aab', 1, 1, '1'), ('aaa', 1, 1, '1'),
                   ('aac', 1, 1, '1') ]),
    ('VCFHyphenSep', [ ('aaa', 1, 1, '1'), ('aaa', 3, 1, '1'),
                       ('aaa', 2, 1, '1') ]),
    ('VCFHyphenSep', [ ('aab', 1, 1, '1'), ('aaa', 1, 1, '1'),
                       ('aac', 1, 1, '1') ]),
    ('VCFParentInt', [ ('aaa', 1, 1, '1'), ('aaa', 1, 3, '1'),
                       ('aaa', 1, 2, '1') ]),
    ('VCFParentInt', [ ('aaa', 1, 1, '1'), ('aaa', 1, 3, '1'),
                       ('aaa', 1, None, '1') ]),
    ('VCFParentIntID', [ ('aaa', 1, 1, '1'), ('aaa', 1, 3, '1'),
                         ('aaa', 1, 2, '1') ]),
    ('VCFParentIntID', [ ('aaa', 1, 1, '1'), ('aaa', 1, 3, '1'),
                         ('aaa', 1, None, '1') ]),
    ('VCFParentStr', [ ('aaa', 1, 1, '3'), ('aaa', 1, 1, '1'),
                       ('aaa', 1, 1, '2') ]),
    ('VCFParentStr', [ ('aaa', 1, 1, '3'), ('aaa', 1, 1, '1'),
                       ('aaa', 1, 1, None) ]),
])
def test_vcfield_orderby_works(modelname, data, testmodels, make_instance):
    """
    Using a VirtualCompField with the `order_by` obj manager method
    should return a QuerySet with instances in the appropriate order:
    ascending or descending, in the same order the VCField values
    would fall if sorted.
    """
    tmodel = testmodels[modelname]
    instances = [make_instance(modelname, *f) for f in data]
    asc_expected = sorted([m.vcf for m in tmodel.objects.all()])
    desc_expected = sorted([m.vcf for m in tmodel.objects.all()], reverse=True)
    asc_qset = tmodel.objects.order_by('vcf')
    desc_qset = tmodel.objects.order_by('-vcf')
    assert [m.vcf for m in asc_qset] == asc_expected
    assert [m.vcf for m in desc_qset] == desc_expected


@pytest.mark.parametrize('modelname', TEST_MODEL_NAMES)
def test_vcfield_delete(modelname, testmodels, make_instance, noise_data):
    """
    Using the `delete` method on an instance should delete the instance
    without raising an error.
    """
    tmodel = testmodels[modelname]
    test_inst = [make_instance(modelname, *f) for f in noise_data(1)][0]
    test_inst_pk = test_inst.pk
    test_inst.delete()
    assert len(tmodel.objects.filter(pk=test_inst_pk)) == 0


@pytest.mark.parametrize('modelname', TEST_MODEL_NAMES)
def test_vcfield_serialization(modelname, testmodels, make_instance,
                               noise_data, do_for_multiple_models):
    """
    Serializing and deserializing a set of random objects should result
    in a second set that is equivalent to the first set, regardless of
    serialization format.
    """
    serial_testmodels = (testmodels['ParentInt'], testmodels['ParentStr'],
                         testmodels[modelname])
    instances = [make_instance(modelname, *f) for f in noise_data(5)]
    og_objects, json_objects, xml_objects = [], [], []
    
    og_objects = do_for_multiple_models(serial_testmodels, 'all')
    json = serializers.serialize('json', og_objects)
    xml = serializers.serialize('xml', og_objects)
    do_for_multiple_models(serial_testmodels, 'delete')

    for obj in serializers.deserialize('json', json):
        obj.save()
    json_objects = do_for_multiple_models(serial_testmodels, 'all')
    do_for_multiple_models(serial_testmodels, 'delete')

    for obj in serializers.deserialize('xml', xml):
        obj.save()
    xml_objects = do_for_multiple_models(serial_testmodels, 'all')
    assert json_objects == og_objects
    assert xml_objects == og_objects
