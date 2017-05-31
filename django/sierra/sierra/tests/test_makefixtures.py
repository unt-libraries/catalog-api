"""
Tests the 'makefixtures' custom management.py command.
"""

import pytest
import ujson

from django.core import serializers
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db.models import Q
from django.utils.six import StringIO

from testmodels import models as m
from sierra.management.commands import makefixtures


# FIXTURES AND TEST DATA

pytestmark = pytest.mark.django_db


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
def confdata():
    onlymodel = {
        'model': 'testmodels.EndNode'
    }
    trace_branches = {
        'model': 'testmodels.ReferenceNode',
        'trace_branches': True
    }
    fullspec = {
        'model': 'testmodels.SelfReferentialNode',
        'filter': {
            'referencenode__name': 'ref1',
        },
        'branches': [
            ['referencenode_set', 'end'],
            ['referencenode_set', 'srn', 'end'],
            ['referencenode_set', 'srn', 'parent', 'end'],
        ]
    }
    return {
        'Only specifies model': onlymodel,
        'No user branches and trace_branches is True': trace_branches,
        'Has user branches and filter': fullspec,
        'All': [onlymodel, trace_branches, fullspec]
    }


@pytest.fixture
def exp_makefixtures_results(scope='module'):
    def onlymodel():
        objs = m.EndNode.objects.order_by('name')
        return serializers.serialize('json', objs)

    def all_results():
        objs = (list(m.EndNode.objects.order_by('name')) +
                list(m.SelfReferentialNode.objects.order_by('name')) +
                list(m.ReferenceNode.objects.order_by('name')) + 
                list(m.ManyToManyNode.objects.order_by('name')) +
                list(m.ThroughNode.objects.order_by('name')))
        return serializers.serialize('json', objs)

    def fullspec():
        end = m.EndNode.objects.filter(
            (Q(referencenode__name='ref1') |
             Q(selfreferentialnode__referencenode__name='ref1') |
             Q(selfreferentialnode__selfreferentialnode__referencenode__name='ref1'))
        ).order_by('name').distinct()

        srn = m.SelfReferentialNode.objects.filter(
            (Q(referencenode__name='ref1') |
             Q(selfreferentialnode__referencenode__name='ref1'))
        ).order_by('name').distinct()

        ref = m.ReferenceNode.objects.filter(name='ref1')
        objs = list(end) + list(srn) + list(ref)
        return serializers.serialize('json', objs)

    return {
        'Only specifies model': onlymodel(),
        'No user branches and trace_branches is True': all_results(),
        'Has user branches and filter': fullspec(),
        'All': all_results()
    }


@pytest.fixture
def nomodel(confdata):
    data = confdata['Has user branches and filter']
    del(data['model'])
    return [data]


@pytest.fixture
def badmodelstr(confdata):
    data = confdata['Has user branches and filter']
    data['model'] = 'SelfReferentialNode'
    return [data]


@pytest.fixture
def invalidmodel(confdata):
    data = confdata['Has user branches and filter']
    data['model'] = 'testmodels.InvalidModel'
    return [data]


@pytest.fixture
def invalidfilter(confdata):
    data = confdata['Has user branches and filter']
    data['filter'] = {'invalid_filter': 'some_value'}
    return [data]


@pytest.fixture
def invalidbranches(confdata):
    data = confdata['Has user branches and filter']
    data['branches'][2] = ['through_set', 'invalid_field']
    return [data]


@pytest.fixture
def multiple(confdata):
    data = confdata['Has user branches and filter']
    data['filter'] = {'invalid_filter': 'some_value'}
    data['branches'][2] = ['through_set', 'invalid_field']
    return [data]


@pytest.fixture
def invalid_confdata(nomodel, badmodelstr, invalidmodel, invalidfilter,
                     invalidbranches, multiple):
    return {
        'No model specified': nomodel,
        'Bad model string': badmodelstr,
        'Invalid model': invalidmodel,
        'Invalid filter': invalidfilter,
        'Invalid branch': invalidbranches,
        'Multiple problems': multiple
    }


@pytest.fixture
def config_file_not_json(make_tmpfile):
    return make_tmpfile('this is not valid json data', 'not.json')


@pytest.fixture
def config_file_json(make_tmpfile):
    data = ('{ "string": "test", "bool": true, "array": ["one", "two"],'
            '"object": { "key": "val" } }')
    return make_tmpfile(data, 'valid.json')


@pytest.fixture
def config_file_bad_spec(make_tmpfile, nomodel):
    return make_tmpfile(ujson.dumps(nomodel), 'bad_spec.json')


@pytest.fixture
def config_file_perfect(make_tmpfile, confdata):
    return make_tmpfile(ujson.dumps(confdata['All']), 'perfect.json')


# TESTS

def test_command_readconfigfile_errors_if_config_file_unreadable():
    """
    Command.read_config_file should raise an IOError if the configuration
    filename given is not a readable file.
    """
    with pytest.raises(IOError):
        makefixtures.Command().read_config_file('no_such_file.json')


def test_command_readconfigfile_errors_if_file_not_json(config_file_not_json):
    """
    Command.read_config_file should raise a ValueError if the config
    file passed in is not valid JSON.
    """
    with pytest.raises(ValueError):
        makefixtures.Command().read_config_file(str(config_file_not_json))


def test_command_readconfigfile_returns_valid_py_object(config_file_json):
    """
    Command.read_config_file should return valid Python data if the
    config file passed in is valid JSON.
    """
    conf = makefixtures.Command().read_config_file(str(config_file_json))
    assert (len(conf) == 1 and conf[0]['string'] == 'test' and
            conf[0]['bool'] == True and
            conf[0]['array'] == ['one', 'two'] and
            conf[0]['object']['key'] == 'val')


def test_command_handle_errors_if_confdata_is_invalid(config_file_bad_spec):
    """
    Command.handle should raise a ConfigIsInvalid error if any of the
    provided configuration data is invalid.
    """
    with pytest.raises(CommandError):
        makefixtures.Command().handle(str(config_file_bad_spec))


@pytest.mark.parametrize('testname', [
    'No model specified',
    'Bad model string',
    'Invalid model',
    'Invalid filter',
    'Invalid branch',
    'Multiple problems'
])
def test_configuration_errors_on_init_of_invalid_confdata(testname,
                                                          invalid_confdata):
    """
    Instantiating a Configuration object using invalid config data
    should raise a ConfigError.
    """
    with pytest.raises(makefixtures.ConfigError):
        makefixtures.Configuration(invalid_confdata[testname])


def test_configuration_stores_correct_data(confdata):
    """
    A Configuration object should store trees and tree_qset data in
    `trees` and `tree_qsets` attributes, respectively.
    """
    config = makefixtures.Configuration(confdata['All'])
    models = [m.EndNode, m.SelfReferentialNode, m.ReferenceNode]
    assert len(config.trees) == 3
    assert len(config.tree_qsets) == 3
    assert all([t.root in models for t in config.trees])
    assert all([t.root in models for t in config.tree_qsets.keys()])
    assert all([qs.model in models for qs in config.tree_qsets.values()])


@pytest.mark.parametrize('mstring', ['', 'EndNode', 'invalid.EndNode',
                                     'testmodels.Invalid'])
def test_getmodelfromstring_errors_on_invalid_string(mstring):
    """
    get_model_from_string should raise a ConfigError if the model
    string passed to it does not return a valid model object.
    """
    with pytest.raises(makefixtures.ConfigError):
        makefixtures.get_model_from_string(mstring)


def test_getmodelfromstring_gets_correct_model():
    """
    get_model_from_string should return the correct model object based
    on the string provided.
    """
    model = makefixtures.get_model_from_string('testmodels.EndNode')
    assert model == m.EndNode


@pytest.mark.parametrize('testname, fname', [
    ('Only specifies model', 'onlymodel.json'),
    ('No user branches and trace_branches is True', 'trace_branches.json'),
    ('Has user branches and filter', 'fullspec.json'),
    ('All', 'all.json')
])
def test_makefixtures_outputs_correct_data(testname, fname, make_tmpfile,
                                           confdata, exp_makefixtures_results):
    """
    Calling the `makefixtures` command should output the expected JSON
    results to stdout.
    """
    out = StringIO()
    cfile = make_tmpfile(ujson.dumps(confdata[testname]), fname)
    call_command('makefixtures', str(cfile), stdout=out)
    assert out.getvalue() == exp_makefixtures_results[testname] + '\n'

