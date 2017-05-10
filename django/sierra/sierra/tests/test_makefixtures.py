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
def config_obj():
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

def test_readconfig_should_error_if_config_not_json(config_file_not_json):
    """
    Command.read_config should raise a ValueError if the configuration
    file passed in is not valid json.
    """
    with pytest.raises(ValueError):
        makefixtures.Command().read_config(str(config_file_not_json))


def test_readconfig_should_error_if_config_file_unreadable():
    """
    Command.read_config should raise an IOError if the configuration
    filename given is not a readable file.
    """
    with pytest.raises(IOError):
        makefixtures.Command().read_config('no_such_file.json')


def test_readconfig_should_return_valid_config_list(config_file_valid):
    """
    Command.read_config should return a valid configuration list,
    even if the provide configuration json file only provides one
    object (instead of a json array of objects).
    """
    config = makefixtures.Command().read_config(str(config_file_valid))
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
def test_tracerelations_should_return_correct_paths_for_a_model(model, exp):
    """
    Command.trace_relations should correctly follow direct many-to-many
    and foreign-key relationships on the given model, resulting in the
    expected list of paths (exp).
    """
    result = makefixtures.Command().trace_relations(model)
    for expected_path in exp:
        assert expected_path in result
    assert len(result) == len(exp)


def test_prepconfig_should_error_if_no_model(config_obj):
    """
    Command.prep_config should raise a ConfigIsInvalid error if any
    config entry does not provide a `model` key.
    """
    del(config_obj['follow_relations']['model'])
    with pytest.raises(makefixtures.ConfigIsInvalid):
        makefixtures.Command().prep_config(config_obj['multi'])


def test_prepconfig_should_error_if_model_is_formatted_wrong(config_obj):
    """
    Command.prep_config should raise a ConfigIsInvalid error if any
    config entry is not formatted as `app.model`.
    """
    config_obj['only_model']['model'] = 'EndNode'
    with pytest.raises(makefixtures.ConfigIsInvalid):
        makefixtures.Command().prep_config(config_obj['multi'])


def test_prepconfig_should_error_if_model_is_invalid(config_obj):
    """
    Command.prep_config should raise a ConfigIsInvalid error if any
    config entry provides a non-existent app or model.
    """
    config_obj['only_model']['model'] = 'testmodels.InvalidModel'
    with pytest.raises(makefixtures.ConfigIsInvalid):
        makefixtures.Command().prep_config(config_obj['multi'])


def test_prepconfig_should_error_if_filter_is_invalid(config_obj):
    """
    Command.prep_config should raise a ConfigIsInvalid error if any
    config entry provides an invalid filter.
    """
    config_obj['full_spec']['filter'] = {'invalid_filter': 'some_value'}
    with pytest.raises(makefixtures.ConfigIsInvalid):
        makefixtures.Command().prep_config(config_obj['multi'])


def test_prepconfig_should_error_if_any_path_value_is_invalid(config_obj):
    """
    Command.prep_config should raise a ConfigIsInvalid error if any
    config entry provides an invalid path entry.
    """
    config_obj['full_spec']['paths'][2] = ['through_set', 'invalid_field']
    with pytest.raises(makefixtures.ConfigIsInvalid):
        makefixtures.Command().prep_config(config_obj['multi'])


def test_prepconfig_should_return_prep_obj_for_valid_config_obj(config_obj):
    """
    Command.prep_config should return a `prepped` dict if all config
    entries are validated.
    """
    prepped = makefixtures.Command().prep_config(config_obj['multi'])
    assert (len(prepped['queries']) == 3 and len(prepped['model_chain']) == 4)


def test_prepconfig_should_return_correct_model_chain(config_obj):
    """
    Command.prep_config should ...
    """
    prepped = makefixtures.Command().prep_config(config_obj['multi'])
    chain = prepped['model_chain']
    end = chain.index(models.EndNode)
    ref = chain.index(models.ReferenceNode)
    srn = chain.index(models.SelfReferentialNode)
    m2m = chain.index(models.ManyToManyNode)
    assert (end < ref and srn < ref and end < m2m and srn < m2m)


@pytest.mark.parametrize('ckey, names', [
    ('only_model', ['end0', 'end1', 'end2']),
    ('follow_relations', ['ref0', 'ref1', 'ref2']),
    ('full_spec', ['srn2']),
    ('related', ['ref1']),
])
def test_prepconfig_should_use_correct_qs_filter(config_obj, ckey, names):
    """
    Command.prep_config should result in querysets that use the
    correct filter, if one is provided.
    """
    p = makefixtures.Command().prep_config([config_obj[ckey]])
    assert names == [obj.name for obj in 
                     p['queries'][0]['qs'].order_by('name')]


def test_prepconfig_queryset_should_use_selectrelated(config_obj):
    """
    Command.prep_config should result in querysets that use
    select_related appropriately, based on what object relations are
    queried.
    """
    p = makefixtures.Command().prep_config([config_obj['related']])
    ref = p['queries'][0]['qs'][0]
    rel = p['queries'][0]['qs'].query.select_related
    assert (rel == {'end': {}, 'srn': {'end': {}, 'parent': {'end': {}}}} and
            ref.end.name == 'end2' and ref.srn.name == 'srn2' and
            ref.srn.end.name == 'end2' and ref.srn.parent.name == 'srn1' and
            ref.srn.parent.end == None)


def test_prepconfig_queryset_should_use_prefetchrelated(config_obj):
    """
    Command.prep_config should result in querysets that use
    prefetch_related appropriately, based on what object relations are
    queried.
    """
    p = makefixtures.Command().prep_config([config_obj['related']])
    ref = p['queries'][0]['qs'][0]
    prefetched = p['queries'][0]['qs']._prefetch_related_lookups
    m2m = ref.m2m.all().order_by('name')
    thr = ref.throughnode_set.all().order_by('name')

    assert (sorted(prefetched) == ['m2m__end', 'throughnode_set__m2m__end'] and
            [obj.name for obj in m2m] == ['m2m0', 'm2m2'] and
            [obj.name for obj in thr] == ['thr2', 'thr3'] and
            [obj.end.name for obj in m2m] == ['end1', 'end0'] and
            [obj.m2m.name for obj in thr] == ['m2m0', 'm2m2'] and
            [obj.m2m.end.name for obj in thr] == ['end1', 'end0'])




