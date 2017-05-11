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


@pytest.fixture
def confdata_onlymodel():
    return {
        'model': 'testmodels.EndNode'
    }


@pytest.fixture
def confdata_followrelations():
    return {
        'model': 'testmodels.ReferenceNode',
        'follow_relations': True
    }


@pytest.fixture
def confdata_fullspec():
    return {
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


@pytest.fixture
def confdata_related():
    return {
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


@pytest.fixture
def confdata(confdata_onlymodel, confdata_followrelations, confdata_fullspec,
             confdata_related):
    return {
        'onlymodel': confdata_onlymodel,
        'followrelations': confdata_followrelations,
        'fullspec': confdata_fullspec,
        'related': confdata_related,
        'multi': [confdata_onlymodel, confdata_followrelations,
                  confdata_fullspec]
    }


@pytest.fixture
def badconfdata_nomodel(confdata):
    del(confdata['fullspec']['model'])
    return confdata


@pytest.fixture
def badconfdata_malformedmodelstr(confdata):
    confdata['fullspec']['model'] = 'SelfReferentialNode'
    return confdata


@pytest.fixture
def badconfdata_invalidmodel(confdata):
    confdata['fullspec']['model'] = 'testmodels.InvalidModel'
    return confdata


@pytest.fixture
def badconfdata_invalidfilter(confdata):
    confdata['fullspec']['filter'] = {'invalid_filter': 'some_value'}
    return confdata


@pytest.fixture
def badconfdata_invalidpaths(confdata):
    confdata['fullspec']['paths'][2] = ['through_set', 'invalid_field']
    return confdata


@pytest.fixture
def badconfdata_multiple(confdata):
    confdata['fullspec']['filter'] = {'invalid_filter': 'some_value'}
    confdata['fullspec']['paths'][2] = ['through_set', 'invalid_field']
    return confdata


@pytest.fixture(scope='module')
def mytempdir(tmpdir_factory):
    return tmpdir_factory.mktemp('data')


@pytest.fixture(scope='module')
def make_tmpfile(mytempdir):
    def make(data, filename):
        path = mytempdir.join(filename)
        with open(str(path), 'w') as fh:
            fh.write(data)
        return path
    return make


@pytest.fixture
def config_file_not_json(make_tmpfile):
    return make_tmpfile('this is not valid json data', 'not.json')


@pytest.fixture
def config_file_json(make_tmpfile):
    data = ('{ "string": "test", "bool": true, "array": ["one", "two"],'
            '"object": { "key": "val" } }')
    return make_tmpfile(data, 'valid.json')


@pytest.fixture
def config_file_not_valid(make_tmpfile, badconfdata_multiple):
    data = ujson.dumps(badconfdata_multiple['multi'])
    return make_tmpfile(data, 'invalid_config.json')


@pytest.fixture
def config_file_valid(make_tmpfile, confdata):
    data = ujason.dumps(confdata['multi'])
    return make_tmpfile(data, 'valid_config.json')



# TESTS

@pytest.mark.parametrize('model, fieldname', [
    ('invalid', 'fieldname'),
    (models.EndNode, 'invalid'),
    (models.EndNode, 'name'),
])
def test_relation_init_raises_error_on_invalid_data(model, fieldname):
    """
    Relation.__init__ should raise a BadRelation error if the provided
    model/fieldname combo is not valid.
    """
    with pytest.raises(makefixtures.BadRelation):
        makefixtures.Relation(model, fieldname)


@pytest.mark.parametrize('model, fn, fk, m2m, indirect', [
    (models.ReferenceNode, 'srn', True, False, False),
    (models.ReferenceNode, 'm2m', False, True, False),
    (models.ReferenceNode, 'throughnode_set', False, False, True),
    (models.EndNode, 'referencenode_set', False, False, True)
])
def test_relation_ismethods_return_right_bools(model, fn, fk, m2m, indirect):
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
def test_relation_targetmodel_has_right_model(model, fn, target):
    """
    Relation.target_model should contain whatever model is on the other
    end of the relation relative to Relation.model.
    """
    rel = makefixtures.Relation(model, fn)
    assert rel.target_model == target


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
    optionally utilizing a supplied "models" list.
    """
    rel = makefixtures.Relation(model, fn)
    assert rel.arrange_models(models) == result


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
    result = [p.fieldnames for p in makefixtures.trace_relations(model)]
    for expected_path in exp:
        assert expected_path in result
    assert len(result) == len(exp)


@pytest.mark.parametrize('baddata, num_errors', [
    (badconfdata_nomodel, 1),
    (badconfdata_malformedmodelstr, 1),
    (badconfdata_invalidmodel, 1),
    (badconfdata_invalidfilter, 1),
    (badconfdata_invalidpaths, 1),
    (badconfdata_multiple, 2),
])
def test_configentry_init_produces_errors_on_invalid_data(confdata, baddata,
                                                          num_errors):
    """
    ConfigEntry.__init__ should record one or more errors in
    ConfigEntry.errors if invalid data is passed.
    """
    good_ce = makefixtures.ConfigEntry(confdata['fullspec'])
    bad_ce = makefixtures.ConfigEntry(baddata(confdata)['fullspec'])
    assert len(good_ce.errors) == 0 and len(bad_ce.errors) == num_errors


def test_configentry_init_returns_valid_configentry(confdata):
    """
    ConfigEntry.__init___ should return a ConfigEntry object with
    model, qs, and paths attributes if the config data is valid. 
    """
    ce = makefixtures.ConfigEntry(confdata['fullspec'])
    assert (len(ce.errors) == 0 and len(ce.paths) == 3 and
            ce.qs is not None and ce.model == models.SelfReferentialNode)


@pytest.mark.parametrize('data, names', [
    (confdata_onlymodel, ['end0', 'end1', 'end2']),
    (confdata_followrelations, ['ref0', 'ref1', 'ref2']),
    (confdata_fullspec, ['srn2']),
    (confdata_related, ['ref1']),
])
def test_configentry_qs_uses_filter(data, names):
    """
    Initializing a new ConfigEntry should result in a qs (QuerySet)
    attribute that use the correct filter, if one is provided.
    """
    ce = makefixtures.ConfigEntry(data())
    assert names == [obj.name for obj in ce.qs.order_by('name')]


def test_configentry_qs_uses_selectrelated(confdata_related):
    """
    Initializing a new ConfigEntry should result in a qs (QuerySet)
    attribute that uses select_related appropriately, based on what
    object relations are queried.
    """
    ce = makefixtures.ConfigEntry(confdata_related)
    ref = ce.qs[0]
    rel = ce.qs.query.select_related
    assert (rel == {'end': {}, 'srn': {'end': {}, 'parent': {'end': {}}}} and
            ref.end.name == 'end2' and ref.srn.name == 'srn2' and
            ref.srn.end.name == 'end2' and ref.srn.parent.name == 'srn1' and
            ref.srn.parent.end == None)


def test_configentry_qs_uses_prefetchrelated(confdata_related):
    """
    Initializing a new ConfigEntry should result in a qs (QuerySet)
    attribute that uses prefetch_related appropriately, based on what
    object relations are queried.
    """
    ce = makefixtures.ConfigEntry(confdata_related)
    ref = ce.qs[0]
    prefetched = ce.qs._prefetch_related_lookups
    m2m = ref.m2m.all().order_by('name')
    thr = ref.throughnode_set.all().order_by('name')

    assert (sorted(prefetched) == ['m2m__end', 'throughnode_set__m2m__end'] and
            [obj.name for obj in m2m] == ['m2m0', 'm2m2'] and
            [obj.name for obj in thr] == ['thr2', 'thr3'] and
            [obj.end.name for obj in m2m] == ['end1', 'end0'] and
            [obj.m2m.name for obj in thr] == ['m2m0', 'm2m2'] and
            [obj.m2m.end.name for obj in thr] == ['end1', 'end0'])


def test_config_init_raises_error_if_confdata_is_invalid(badconfdata_nomodel):
    """
    Config.__init__ should raise a ConfigIsInvalid error if any of the
    provided configuration data is invalid.
    """
    with pytest.raises(makefixtures.ConfigIsInvalid):
        conf = makefixtures.Config(badconfdata_nomodel['multi'])


def test_config_init_returns_valid_config_obj(confdata):
    """
    Config.__init__ should create a valid Config object if valid config
    data is provided.
    """
    conf = makefixtures.Config(confdata['multi'])
    assert (len(conf.errors) == 0 and len(conf) == 3)


def test_config_dependencies_contains_correct_model_order(confdata):
    """
    Config.dependencies should have models in dependency order.
    """
    conf = makefixtures.Config(confdata['multi'])
    dep = conf.dependencies
    end = dep.index(models.EndNode)
    ref = dep.index(models.ReferenceNode)
    srn = dep.index(models.SelfReferentialNode)
    m2m = dep.index(models.ManyToManyNode)
    assert (end < ref and srn < ref and end < m2m and srn < m2m)


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


def test_readconfigfile_returns_valid_config_object(config_file_json):
    """
    Command.read_config_file should return valid Python data.
    """
    conf = makefixtures.Command().read_config_file(str(config_file_json))
    assert (len(conf) == 1 and conf[0]['string'] == 'test' and
            conf[0]['bool'] == True and
            conf[0]['array'] == ['one', 'two'] and
            conf[0]['object']['key'] == 'val')


def test_command_handle_raises_err_if_confdata_invalid(config_file_not_valid):
    """
    Command.handle should raise a ConfigIsInvalid error if any of the
    provided configuration data is invalid.
    """
    with pytest.raises(makefixtures.ConfigIsInvalid):
        makefixtures.Command().handle(str(config_file_not_valid))
