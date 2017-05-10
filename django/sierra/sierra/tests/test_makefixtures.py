"""
Tests the 'makefixtures' custom management.py command.
"""

import pytest

import ujson

from testmodels import models
from django.conf import settings
from django.core.management import call_command
from django.utils.six import StringIO

from sierra.management.commands import makefixtures

# FIXTURES AND TEST DATA

pytestmark = pytest.mark.django_db

@pytest.fixture(scope='module')
def mytempdir(tmpdir_factory):
    return tmpdir_factory.mktemp('data')


def tmpfile(data, filename):
    @pytest.fixture(scope='module')
    def file_fixture(mytempdir):
        path = mytempdir.join(filename)
        with open(str(path), 'w') as fh:
            fh.write(data)
        return path
    return file_fixture


not_json = "this is not json"
valid_json = """{
    "string": "test",
    "bool": true,
    "array": ["one", "two"],
    "object": { "key": "val" }
}

"""
config_file_not_json = tmpfile(not_json, 'not.json')
config_file_valid = tmpfile(valid_json, 'valid.json')


@pytest.fixture(scope='function')
def config_data():
    only_model = {
        'model': 'testmodels.EndNode'
    }
    follow_relations = {
        'model': 'testmodels.ReferenceNode',
        'follow_relations': True
    }
    full_spec = {
        'model': 'testmodels.SelfReferentialNode',
        'filter': {
            'referencenode__name': 'ref1',
        },
        'paths': [
            ['referencenode_set', 'end'],
            ['referencenode_set', 'srn', 'end'],
            ['referencenode_set', 'srn', 'parent', 'end'],
        ]
    }
    related = {
        'model': 'testmodels.ReferenceNode',
        'filter': {
            'name': 'ref1',
        },
        'paths': [
            ['srn', 'end'],
            ['srn', 'parent', 'end'],
            ['end'],
            ['m2m', 'end'],
            ['throughnode_set', 'm2m', 'end']
        ]
    }

    return {'only_model': only_model, 'follow_relations': follow_relations,
            'full_spec': full_spec, 'related': related,
            'multi': [only_model, follow_relations, full_spec]}


# TESTS

def test_relation_init_errors_with_invalid_model():
    """
    Relation.__init__ should raise a BadRelation error if the provided
    model is invalid.
    """
    with pytest.raises(makefixtures.BadRelation):
        makefixtures.Relation('invalid', 'fieldname')


def test_relation_init_errors_with_nonexistent_field():
    """
    Relation.__init__ should raise a BadRelation error if the provided
    fieldname is nonexistent.
    """
    with pytest.raises(makefixtures.BadRelation):
        makefixtures.Relation(models.EndNode, 'invalid')


def test_relation_init_errors_with_nonrelation_field():
    """
    Relation.__init__ should raise a BadRelation error if the provided
    fieldname is not a relation field.
    """
    with pytest.raises(makefixtures.BadRelation):
        makefixtures.Relation(models.EndNode, 'name')


@pytest.mark.parametrize('model, fn, fk, m2m, indirect', [
    (models.ReferenceNode, 'srn', True, False, False),
    (models.ReferenceNode, 'm2m', False, True, False),
    (models.ReferenceNode, 'throughnode_set', False, False, True),
    (models.EndNode, 'referencenode_set', False, False, True)
])
def test_relation_is_methods(model, fn, fk, m2m, indirect):
    """
    All "is" methods on Relation objects should return the correct
    truth values for the type of relation that is represented.
    """
    rel = makefixtures.Relation(model, fn)
    assert (rel.is_foreign_key() == fk and rel.is_many_to_many() == m2m and
            rel.is_indirect() == indirect)


@pytest.mark.parametrize('model, fn, target', [
    (models.ReferenceNode, 'srn', models.SelfReferentialNode),
    (models.ReferenceNode, 'm2m', models.ManyToManyNode),
    (models.ReferenceNode, 'throughnode_set', models.ThroughNode),
    (models.EndNode, 'referencenode_set', models.ReferenceNode)
])
def test_relation_gettargetmodel_returns_correct_model(model, fn, target):
    """
    Relation.get_target_model should return whatever model is on the
    other end of the relation relative to Relation.model.
    """
    rel = makefixtures.Relation(model, fn)
    assert rel.get_target_model() == target


@pytest.mark.parametrize('model, fn, models, result', [
    (models.ReferenceNode, 'srn', None, [models.SelfReferentialNode,
                                         models.ReferenceNode]),
    (models.ReferenceNode, 'srn',
     [models.SelfReferentialNode],
     [models.SelfReferentialNode, models.ReferenceNode]),
    (models.ReferenceNode, 'srn',
     [models.ReferenceNode],
     [models.SelfReferentialNode, models.ReferenceNode]),
    (models.ReferenceNode, 'srn',
     [models.ReferenceNode, models.SelfReferentialNode],
     [models.SelfReferentialNode, models.ReferenceNode]),
    (models.ReferenceNode, 'srn',
     [models.EndNode, models.ReferenceNode, models.SelfReferentialNode],
     [models.EndNode, models.SelfReferentialNode, models.ReferenceNode]),
    (models.ReferenceNode, 'srn',
     [models.ReferenceNode, models.EndNode, models.SelfReferentialNode],
     [models.SelfReferentialNode, models.ReferenceNode, models.EndNode]),
])
def test_relation_arrangemodels_dependency_order(model, fn, models, result):
    """
    Relation.arrange_models should return models in dependency order,
    optionally utilizing a supplied "models" list. If "models" is
    supplied, and the models in the Relation are in the list, they
    should be rearranged as needed. If one or both of the models in
    the Relation are not in "models", they should be added and arranged
    in the correct order.
    """
    rel = makefixtures.Relation(model, fn)
    assert rel.arrange_models(models) == result


def test_readconfigfile_errors_if_file_not_json(config_file_not_json):
    """
    Command.read_config_file should raise a ValueError if the config
    file passed in is not valid json.
    """
    with pytest.raises(ValueError):
        makefixtures.Command().read_config_file(str(config_file_not_json))


def test_readconfigfile_errors_if_config_file_unreadable():
    """
    Command.read_config_file should raise an IOError if the configuration
    filename given is not a readable file.
    """
    with pytest.raises(IOError):
        makefixtures.Command().read_config_file('no_such_file.json')


def test_readconfigfile_returns_valid_config_object(config_file_valid):
    """
    Command.read_config_file should return valid Python data.
    """
    config = makefixtures.Command().read_config_file(str(config_file_valid))
    assert (len(config) == 1 and config[0]['string'] == 'test' and
            config[0]['bool'] == True and
            config[0]['array'] == ['one', 'two'] and
            config[0]['object']['key'] == 'val')


@pytest.mark.parametrize('model, exp', [
    (models.ReferenceNode, [['end'], ['srn', 'end'], ['srn', 'parent', 'end'],
                            ['m2m', 'end']]),
    (models.ThroughNode, [['ref', 'end'], ['ref', 'srn', 'end'],
                          ['ref', 'srn', 'parent', 'end'],
                          ['ref', 'm2m', 'end'], ['m2m', 'end']]),
    (models.EndNode, []),
    (models.SelfReferentialNode, [['end'], ['parent', 'end']]),
    (models.ManyToManyNode, [['end']])
])
def test_tracerelations_returns_correct_paths_for_a_model(model, exp):
    """
    trace_relations should correctly follow direct many-to-many
    and foreign-key relationships on the given model, resulting in the
    expected list of paths (exp).
    """
    result = [p.path_fields for p in makefixtures.trace_relations(model)]
    for expected_path in exp:
        assert expected_path in result
    assert len(result) == len(exp)


def test_config_init_produces_errors_if_no_model(config_data):
    """
    Config.__init__ should record an error in Config.errors if any
    user-supplied config entry does not provide a `model` key.
    """
    good_conf = makefixtures.Config(config_data['multi'])
    del(config_data['follow_relations']['model'])
    bad_conf = makefixtures.Config(config_data['multi'])
    assert len(good_conf.errors) == 0 and len(bad_conf.errors) == 1


def test_config_init_produces_errors_if_model_is_formatted_wrong(config_data):
    """
    Config.__init__ should record an error in Config.errors if any
    config entry is not formatted as `app.model`.
    """
    good_conf = makefixtures.Config(config_data['multi'])
    config_data['only_model']['model'] = 'EndNode'
    bad_conf = makefixtures.Config(config_data['multi'])
    assert len(good_conf.errors) == 0 and len(bad_conf.errors) == 1


def test_config_init_produces_errors_if_model_is_invalid(config_data):
    """
    Config.__init__ should record an error in Config.errors if any
    config entry provides a non-existent app or model.
    """
    good_conf = makefixtures.Config(config_data['multi'])
    config_data['only_model']['model'] = 'testmodels.InvalidModel'
    bad_conf = makefixtures.Config(config_data['multi'])
    assert len(good_conf.errors) == 0 and len(bad_conf.errors) == 1


def test_config_init_produces_errors_if_filter_is_invalid(config_data):
    """
    Config.__init__ should record an error in Config.errors if any
    config entry provides an invalid filter.
    """
    good_conf = makefixtures.Config(config_data['multi'])
    config_data['full_spec']['filter'] = {'invalid_filter': 'some_value'}
    bad_conf = makefixtures.Config(config_data['multi'])
    assert len(good_conf.errors) == 0 and len(bad_conf.errors) == 1


def test_config_init_produces_errors_if_any_path_value_is_invalid(config_data):
    """
    Config.__init__ should record an error in Config.errors if any
    config entry provides an invalid path entry.
    """
    good_conf = makefixtures.Config(config_data['multi'])
    config_data['full_spec']['paths'][2] = ['through_set', 'invalid_field']
    bad_conf = makefixtures.Config(config_data['multi'])
    assert len(good_conf.errors) == 0 and len(bad_conf.errors) == 1


def test_config_init_returns_valid_config_obj(config_data):
    """
    Config.__init__ should create a valid Config object with valid
    config data.
    """
    conf = makefixtures.Config(config_data['multi'])
    assert (len(conf.errors) == 0 and len(conf.entries) == 3)


def test_config_getdependencies_returns_correct_model_order(config_data):
    """
    Config.get_dependencies should return models in dependency order.
    """
    conf = makefixtures.Config(config_data['multi'])
    dep = conf.get_dependencies()
    end = dep.index(models.EndNode)
    ref = dep.index(models.ReferenceNode)
    srn = dep.index(models.SelfReferentialNode)
    m2m = dep.index(models.ManyToManyNode)
    assert (end < ref and srn < ref and end < m2m and srn < m2m)


@pytest.mark.parametrize('ckey, names', [
    ('only_model', ['end0', 'end1', 'end2']),
    ('follow_relations', ['ref0', 'ref1', 'ref2']),
    ('full_spec', ['srn2']),
    ('related', ['ref1']),
])
def test_configentry_getfilteredqueryset_uses_qs_filter(config_data, ckey,
                                                        names):
    """
    ConfigEntry.get_filtered_queryset should result in querysets that
    use the correct filter, if one is provided.
    """
    conf_entry = makefixtures.ConfigEntry(config_data[ckey])
    assert names == [obj.name for obj in conf_entry.qs.order_by('name')]


def test_configentry_prepqsrelations_qs_uses_selectrelated(config_data):
    """
    ConfigEntry.prep_qs_relations should result in querysets that use
    select_related appropriately, based on what object relations are
    queried.
    """
    conf_entry = makefixtures.ConfigEntry(config_data['related'])
    ref = conf_entry.qs[0]
    rel = conf_entry.qs.query.select_related
    assert (rel == {'end': {}, 'srn': {'end': {}, 'parent': {'end': {}}}} and
            ref.end.name == 'end2' and ref.srn.name == 'srn2' and
            ref.srn.end.name == 'end2' and ref.srn.parent.name == 'srn1' and
            ref.srn.parent.end == None)


def test_configentry_prepqsrelations_qs_uses_prefetchrelated(config_data):
    """
    ConfigEntry.prep_qs_relations should result in querysets that use
    prefetch_related appropriately, based on what object relations are
    queried.
    """
    conf_entry = makefixtures.ConfigEntry(config_data['related'])
    ref = conf_entry.qs[0]
    prefetched = conf_entry.qs._prefetch_related_lookups
    m2m = ref.m2m.all().order_by('name')
    thr = ref.throughnode_set.all().order_by('name')

    assert (sorted(prefetched) == ['m2m__end', 'throughnode_set__m2m__end'] and
            [obj.name for obj in m2m] == ['m2m0', 'm2m2'] and
            [obj.name for obj in thr] == ['thr2', 'thr3'] and
            [obj.end.name for obj in m2m] == ['end1', 'end0'] and
            [obj.m2m.name for obj in thr] == ['m2m0', 'm2m2'] and
            [obj.m2m.end.name for obj in thr] == ['end1', 'end0'])
